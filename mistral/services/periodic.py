# -*- coding: utf-8 -*-
#
# Copyright 2013 - Mirantis, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

import copy
import datetime
import requests

from oslo_config import cfg
from oslo_log import log as logging
from oslo_service import periodic_task
from oslo_service import threadgroup

from mistral import context as auth_ctx
from mistral.db.v2 import api as db_api_v2
from mistral.engine.rpc_backend import rpc
from mistral import exceptions as exc
from mistral.services import delay_tolerant_workload as dtw
from mistral.services import security
from mistral.services import triggers

# threshold for work to be considered long term (in minutes)
MINIMUM_LONG_TERM_WORKLOAD = 360

LOG = logging.getLogger(__name__)

CONF = cfg.CONF

# {periodic_task: thread_group}
_periodic_tasks = {}

# Make sure to import 'auth_enable' option before using it.
CONF.import_opt('dtw_scheduler_last_minute', 'mistral.config', group='engine')


class MistralPeriodicTasks(periodic_task.PeriodicTasks):
    @periodic_task.periodic_task(spacing=1, run_immediately=True)
    def process_cron_triggers_v2(self, ctx):
        for t in triggers.get_next_cron_triggers():
            LOG.debug("Processing cron trigger: %s" % t)

            # Setup admin context before schedule triggers.
            ctx = security.create_context(t.trust_id, t.project_id)

            auth_ctx.set_ctx(ctx)

            LOG.debug("Cron trigger security context: %s" % ctx)

            try:
                # Try to advance the cron trigger next_execution_time and
                # remaining_executions if relevant.
                modified = advance_cron_trigger(t)

                # If cron trigger was not already modified by another engine.
                if modified:
                    LOG.debug(
                        "Starting workflow '%s' by cron trigger '%s'",
                        t.workflow.name, t.name
                    )

                    rpc.get_engine_client().start_workflow(
                        t.workflow.name,
                        t.workflow_input,
                        description="Workflow execution created "
                                    "by cron trigger.",
                        **t.workflow_params
                    )
            except Exception:
                # Log and continue to next cron trigger.
                LOG.exception("Failed to process cron trigger %s" % str(t))
            finally:
                auth_ctx.set_ctx(None)

    def _dtw_schedule_immediately(self, ctx):

        for d in dtw.get_delay_tolerant_workload_with_execution():
            # Setup admin context before schedule triggers.
            ctx = security.create_context(d.trust_id, d.project_id)

            auth_ctx.set_ctx(ctx)

            LOG.debug("Delay tolerant workload security context: %s" % ctx)

            try:
                # execute the workload

                db_api_v2.update_delay_tolerant_workload(
                    d.name,
                    {'executed': True}
                )

                rpc.get_engine_client().start_workflow(
                    d.workflow.name,
                    d.workflow_input,
                    description="DTW Workflow execution created.",
                    **d.workflow_params
                )
            except Exception:
                # Log and continue to next cron trigger.
                LOG.exception(
                    "Failed to process delay tolerant workload %s" % str(d))
            finally:
                auth_ctx.set_ctx(None)

    def _dtw_last_minute_scheduling(self, ctx):

        for d in dtw.get_unscheduled_delay_tolerant_workload():
            LOG.debug("Processing delay tolerant workload: %s" % d)

            # Setup admin context before schedule triggers.
            ctx = security.create_context(d.trust_id, d.project_id)

            auth_ctx.set_ctx(ctx)

            LOG.debug("Delay tolerant workload security context: %s" % ctx)

            db_api_v2.update_delay_tolerant_workload(
                    d.name,
                    {'scheduled': True}
                )

            # calculate last time for running this - deadline less the
            # duration of the work
            # TODO(murp): check the status of the security context on this
            # TODO(murp): convert job_duration to timedelta
            start_time = d.deadline \
                - datetime.timedelta(minutes=d.job_duration)

            triggers.create_cron_trigger(d.name, d.workflow_name,
                                         d.workflow_input,
                                         workflow_params=d.workflow_params,
                                         count=1,
                                         first_time=start_time,
                                         start_time=start_time,
                                         workflow_id=d.workflow_id,
                                         trust_id=d.trust_id)

    def _find_optimal_start_time_with_data(self,
                                           current_time,
                                           energy_prices,
                                           job_duration,
                                           deadline):
        """This function determines optimal start time for job with given data.

        This function iterates over all the scheduling times before the
        deadline and determines the best time to schedule the job in terms of
        lowest mean energy price.
        """

        # merge energy prices
        ep = dict()
        ep.update(energy_prices['intra-day'])
        ep.update(energy_prices['day-ahead'])
        ref_ep = copy.copy(ep)

        today = current_time.date()

        # prices in the past are not valid - remove from dict
        # we also remove the current time window - this should be reviewed
        for i in range(0, current_time.hour + 1):
            k = datetime.datetime.combine(today, datetime.time(hour=i))
            del ep[k]

        # this is the last time for which we have energy prices
        final_time = (datetime.datetime.combine(today, datetime.time(0)) +
                      datetime.timedelta(days=2))
        if (deadline - datetime.timedelta(minutes=job_duration)) < final_time:
            # remove the future times which are not valid - those past
            # deadline-job_duration are out
            # TODO(murp): check if rounding on job_duration is an issue
            iter_time_approx = (deadline -
                                datetime.timedelta(minutes=job_duration))
            iter_time = iter_time_approx \
                - datetime.timedelta(minutes=iter_time_approx.minute,
                                     seconds=iter_time_approx.second) \
                + datetime.timedelta(hours=1)

            while iter_time < final_time:
                del ep[iter_time]
                iter_time += datetime.timedelta(hours=1)
        # next remove times which are too late to start because they
        # will result in job finishing after deadline...

        job_duration_hours = (job_duration * 1.0) / 60
        minimum_price = -1
        minimum_time = None

        for t in ep:
            job_cost = 0
            for i in range(0, int(job_duration_hours)):
                job_cost += ref_ep[t + datetime.timedelta(hours=i)]
            if minimum_price == -1 or job_cost < minimum_price:
                minimum_price = job_cost
                minimum_time = t

        return minimum_time

    def _get_energy_prices(self):
        """This function queries the configured API for energy prices.

        Currently, the API is configured locally, but this should
        be a configuration option.
        """
        # this needs to be written...
        try:
            return requests.get('http://localhost:9500/energy-price').json()
        except requests.ConnectionError:
            return None

    def _determine_optimal_scheduling(self, job_duration, deadline):
        """This function determines the when to schedule job.

        It is based on the current information relating to energy
        consumption. Currently, it does not take into account the
        rest of the DTW, meaning that it is entirely possible it
        may be distributed in a very bursty manner.
        """

        # get the energy prices
        energy_prices = self._get_energy_prices()

        if not energy_prices:
            return datetime.datetime.now() + datetime.timedelta(minutes=2)

        # get the current time
        current_time = datetime.datetime.now()

        optimal_start_time = \
            self._find_optimal_start_time_with_data(current_time,
                                                    energy_prices,
                                                    job_duration,
                                                    deadline)

        return optimal_start_time

    def _dtw_energy_aware_scheduling(self, ctx):
        """This function schedules the workload in an energy efficient way.

        This function considers two types of workloads:
        - short term workloads (duration < 6 hrs)
        - long term workloads (duration > 6 hrs)

        For the short term workload, we determine the most suitable time to
        run it given the deadline and the variation in energy price. For the
        long term workload, we consider it too long term to be able to gain
        from energy aware scheduling.

        Note that the differentiation between short term and long term
        workloads is somewhat arbitrary right now and needs further
        consideration.
        """

        for d in dtw.get_unscheduled_delay_tolerant_workload():
            LOG.debug("Processing delay tolerant workload: %s" % d)

            # Setup admin context before schedule triggers.
            ctx = security.create_context(d.trust_id, d.project_id)

            auth_ctx.set_ctx(ctx)

            LOG.debug("Delay tolerant workload security context: %s" % ctx)

            if d.job_duration > MINIMUM_LONG_TERM_WORKLOAD:
                # it is a long term workload - schedule immediately
                try:
                    # execute the workload

                    db_api_v2.update_delay_tolerant_workload(
                        d.name,
                        {'executed': True}
                    )

                    rpc.get_engine_client().start_workflow(
                        d.workflow.name,
                        d.workflow_input,
                        description="DTW Workflow execution created.",
                        **d.workflow_params
                    )
                except Exception:
                    # Log and continue to next cron trigger.
                    LOG.exception(
                        "Failed to process delay tolerant workload %s" %
                        str(d))
                finally:
                    auth_ctx.set_ctx(None)
            else:
                # it is a short term workload
                scheduling_time = \
                    self._determine_optimal_scheduling(d.job_duration,
                                                       d.deadline)

                triggers.create_cron_trigger(d.name, d.workflow_name,
                                             d.workflow_input,
                                             workflow_params=d.workflow_params,
                                             count=1,
                                             first_time=scheduling_time,
                                             start_time=scheduling_time,
                                             workflow_id=d.workflow_id)

    @periodic_task.periodic_task(spacing=1, run_immediately=True)
    def process_delay_tolerant_workload(self, ctx):
        """This function schedules delay tolerant workload.

        In this initial, basic version, the function just determines if there
        is new DTW and schedules it immediately.
        """

        if CONF.engine.dtw_scheduler_mode == 'immediately':
            self._dtw_schedule_immediately(ctx)
        elif CONF.engine.dtw_scheduler_mode == 'last-minute':
            self._dtw_last_minute_scheduling(ctx)
        elif CONF.engine.dtw_scheduler_mode == 'energy-aware':
            self._dtw_energy_aware_scheduling(ctx)
        else:
            raise AttributeError


def advance_cron_trigger(t):
    modified_count = 0

    try:
        # If the cron trigger is defined with limited execution count.
        if t.remaining_executions is not None and t.remaining_executions > 0:
            t.remaining_executions -= 1

        # If this is the last execution.
        if t.remaining_executions == 0:
            modified_count = db_api_v2.delete_cron_trigger(t.name)
        else:  # if remaining execution = None or > 0.
            next_time = triggers.get_next_execution_time(
                t.pattern,
                t.next_execution_time
            )

            # Update the cron trigger with next execution details
            # only if it wasn't already updated by a different process.
            updated, modified_count = db_api_v2.update_cron_trigger(
                t.name,
                {
                    'next_execution_time': next_time,
                    'remaining_executions': t.remaining_executions
                },
                query_filter={
                    'next_execution_time': t.next_execution_time
                }
            )
    except exc.DBEntityNotFoundError as e:
        # Cron trigger was probably already deleted by a different process.
        LOG.debug(
            "Cron trigger named '%s' does not exist anymore: %s",
            t.name, str(e)
        )

    # Return True if this engine was able to modify the cron trigger in DB.
    return modified_count > 0


def setup():
    tg = threadgroup.ThreadGroup()
    pt = MistralPeriodicTasks(CONF)

    ctx = auth_ctx.MistralContext(
        user_id=None,
        project_id=None,
        auth_token=None,
        is_admin=True
    )

    tg.add_dynamic_timer(
        pt.run_periodic_tasks,
        initial_delay=None,
        periodic_interval_max=1,
        context=ctx
    )

    _periodic_tasks[pt] = tg

    return tg


def stop_all_periodic_tasks():
    for pt, tg in _periodic_tasks.items():
        tg.stop()
        del _periodic_tasks[pt]
