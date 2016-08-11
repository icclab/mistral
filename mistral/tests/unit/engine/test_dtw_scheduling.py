# Copyright (c) 2016. Zuercher Hochschule fuer Angewandte Wissenschaften
# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
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
import datetime

import mock
from oslo_config import cfg

from mistral.db.v2 import api as db_api
from mistral.services import delay_tolerant_workload as dtw
from mistral.services import periodic
from mistral.services import security
from mistral.services import workflows
from mistral.tests.unit.engine import base
from mistral import utils

import test_data

WORKFLOW_LIST = """
---
version: '2.0'

my_wf:
  type: direct

  tasks:
    task1:
      action: std.echo output='Hi!'
"""


class ProcessDelayTolerantWorkload(base.EngineTestCase):
    @mock.patch.object(security,
                       'create_trust',
                       type('trust', (object,), {'id': 'my_trust_id'}))
    def test_start_workflow(self):
        cfg.CONF.set_default('auth_enable', True, group='pecan')
        cfg.CONF.set_default('dtw_scheduler_mode', 'immediately',
                             group='engine')

        wf = workflows.create_workflows(WORKFLOW_LIST)[0]

        d = dtw.create_delay_tolerant_workload(
            'dtw-%s' % utils.generate_unicode_uuid(),
            wf.name,
            {},
            {},
            (datetime.datetime.now() + datetime.timedelta(hours=2))
            .strftime('%Y-%m-%dT%H:%M:%S'),
            None,
            None
        )

        self.assertEqual('my_trust_id', d.trust_id)

        cfg.CONF.set_default('auth_enable', False, group='pecan')

        unscheduled_workload = dtw.get_unscheduled_delay_tolerant_workload()
        self.assertEqual(1, len(unscheduled_workload))
        self.assertEqual(d.name, unscheduled_workload[0].name)
        self.assertEqual(d.deadline, unscheduled_workload[0].deadline)

        periodic.MistralPeriodicTasks(
            cfg.CONF).process_delay_tolerant_workload(None)

        unscheduled_workload_after = dtw \
            .get_unscheduled_delay_tolerant_workload()
        self.assertEqual(0, len(unscheduled_workload_after))

        # Checking the workflow was executed, by
        # verifying that the status has changed to executed=True
        executed_workload = db_api.get_delay_tolerant_workload(d.name)
        self.assertEqual(executed_workload.executed, True)

    def test_workflow_without_auth(self):
        cfg.CONF.set_default('auth_enable', False, group='pecan')
        cfg.CONF.set_default('dtw_scheduler_last_minute', False,
                             group='engine')

        wf = workflows.create_workflows(WORKFLOW_LIST)[0]

        d = dtw.create_delay_tolerant_workload(
            'dtw-%s' % utils.generate_unicode_uuid(),
            wf.name,
            {},
            {},
            (datetime.datetime.now() + datetime.timedelta(hours=2))
            .strftime('%Y-%m-%dT%H:%M:%S'),
            None,
            None
        )

        unscheduled_workload = dtw.get_unscheduled_delay_tolerant_workload()
        self.assertEqual(1, len(unscheduled_workload))
        self.assertEqual(d.deadline, unscheduled_workload[0].deadline)

        periodic.MistralPeriodicTasks(
            cfg.CONF).process_delay_tolerant_workload(None)

        unscheduled_workload = dtw.get_unscheduled_delay_tolerant_workload()
        self.assertEqual(0, len(unscheduled_workload))

        executed_workload = db_api.get_delay_tolerant_workload(d.name)
        self.assertEqual(executed_workload.executed, True)

    @mock.patch.object(security,
                       'create_trust',
                       type('trust', (object,), {'id': 'my_trust_id'}))
    def test_last_minute_scheduled_workload(self):
        cfg.CONF.set_default('auth_enable', True, group='pecan')
        cfg.CONF.set_default('dtw_scheduler_mode', "last-minute",
                             group='engine')

        wf = workflows.create_workflows(WORKFLOW_LIST)[0]

        name = 'dtw-%s' % utils.generate_unicode_uuid()

        d = dtw.create_delay_tolerant_workload(
            name,
            wf.name,
            {},
            {},
            (datetime.datetime.now() + datetime.timedelta(days=2))
            .strftime('%Y-%m-%dT%H:%M:%S'),
            600,
            None
        )

        self.assertEqual('my_trust_id', d.trust_id)

        cfg.CONF.set_default('auth_enable', False, group='pecan')

        unscheduled_workload = dtw.get_unscheduled_delay_tolerant_workload()
        self.assertEqual(1, len(unscheduled_workload))
        self.assertEqual(d.name, unscheduled_workload[0].name)
        self.assertEqual(d.deadline, unscheduled_workload[0].deadline)

        periodic.MistralPeriodicTasks(
            cfg.CONF).process_delay_tolerant_workload(None)

        unscheduled_workload_after = dtw \
            .get_unscheduled_delay_tolerant_workload()
        self.assertEqual(1, len(unscheduled_workload_after))

        # so we should check if we have a cron trigger associated with this
        # workload now
        cron_trigger_db = db_api.get_cron_trigger(d.name)

        self.assertIsNotNone(cron_trigger_db)
        self.assertEqual(name, cron_trigger_db.name)

    def test_find_optimal_start_time_with_data(self):
        cfg.CONF.set_default('auth_enable', True, group='pecan')
        cfg.CONF.set_default('dtw_scheduler_mode', 'last-minute',
                             group='engine')

        current_time = datetime.datetime.strptime("2016-07-06T15:43:00",
                                                  "%Y-%m-%dT%H:%M:%S")

        # job duration in minutes
        job_duration = 75
        deadline = datetime.datetime.strptime("2016-07-06T23:00:00",
                                              "%Y-%m-%dT%H:%M:%S")

        scheduling_time = \
            periodic.MistralPeriodicTasks(cfg.CONF)\
            ._find_optimal_start_time_with_data(current_time,
                                                test_data.ENERGY_PRICES,
                                                job_duration, deadline)
        expected_time = datetime.datetime.strptime("2016-07-06T21:00:00",
                                                   "%Y-%m-%dT%H:%M:%S")
        self.assertEqual(scheduling_time, expected_time)

        current_time = datetime.datetime.strptime("2016-07-06T07:43:00",
                                                  "%Y-%m-%dT%H:%M:%S")

        # job duration in minutes
        job_duration = 150
        deadline = datetime.datetime.strptime("2016-07-06T23:00:00",
                                              "%Y-%m-%dT%H:%M:%S")
        scheduling_time = \
            periodic.MistralPeriodicTasks(cfg.CONF)\
            ._find_optimal_start_time_with_data(current_time,
                                                test_data.ENERGY_PRICES,
                                                job_duration, deadline)
        expected_time = datetime.datetime.strptime("2016-07-06T15:00:00",
                                                   "%Y-%m-%dT%H:%M:%S")
        self.assertEqual(scheduling_time, expected_time)

    @mock.patch.object(periodic.MistralPeriodicTasks,
                       '_dtw_schedule_immediately')
    @mock.patch.object(periodic.MistralPeriodicTasks,
                       '_dtw_last_minute_scheduling')
    @mock.patch.object(periodic.MistralPeriodicTasks,
                       '_dtw_energy_aware_scheduling')
    def test_periodic_conf_inputs(self, mock_energy, mock_last, mock_immed):
        cfg.CONF.set_default('dtw_scheduler_mode',
                             'energy-aware',
                             group='engine')
        cfg.CONF.set_default('dtw_scheduler_last_minute',
                             False, group='engine')

        periodic.MistralPeriodicTasks(
            cfg.CONF).process_delay_tolerant_workload(None)
        self.assertFalse(mock_immed.called)
        self.assertFalse(mock_last.called)
        self.assertTrue(mock_energy.called)

    def test_process_delay_tolerant_workload_raise_error(self):
        cfg.CONF.set_default('dtw_scheduler_mode',
                             'ERROR',
                             group='engine')
        self.assertRaises(
            AttributeError,
            periodic.MistralPeriodicTasks(
                cfg.CONF).process_delay_tolerant_workload,
            None
        )

    def test_get_energy_prices(self):
        result = periodic.MistralPeriodicTasks(cfg.CONF)._get_energy_prices()
        self.assertIsNone(result)

    @mock.patch.object(periodic.MistralPeriodicTasks,
                       '_get_energy_prices', mock.MagicMock(return_value=None))
    def test_determine_optimal_scheduling_returns_current_time(self):
        # Method _get_energy_price returns None, the
        # _determine_optimal_scheduling function then should return the
        # current time + 2 mins
        datetime_result = periodic.MistralPeriodicTasks(
            cfg.CONF)._determine_optimal_scheduling(None, None).strftime(
            "%Y-%m-%dT%H:%M:%S")

        datetime_expected = \
            (datetime.datetime.now() + datetime.timedelta(minutes=2)).strftime(
                "%Y-%m-%dT%H:%M:%S")
        self.assertEqual(datetime_expected, datetime_result)

    @mock.patch.object(periodic.MistralPeriodicTasks,
                       '_get_energy_prices',
                       mock.MagicMock(return_value=test_data.ENERGY_PRICES))
    def test_determine_optimal_scheduling_returns_optimal_time(self):
        current_time = datetime.datetime.strptime("2016-07-06T15:43:00",
                                                  "%Y-%m-%dT%H:%M:%S")

        # job duration in minutes
        job_duration = 75
        deadline = datetime.datetime.strptime("2016-07-06T23:00:00",
                                              "%Y-%m-%dT%H:%M:%S")
        scheduling_time = \
            periodic.MistralPeriodicTasks(cfg.CONF)\
            ._find_optimal_start_time_with_data(current_time,
                                                test_data.ENERGY_PRICES,
                                                job_duration, deadline)
        expected_time = datetime.datetime.strptime("2016-07-06T21:00:00",
                                                   "%Y-%m-%dT%H:%M:%S")
        self.assertEqual(scheduling_time, expected_time)
