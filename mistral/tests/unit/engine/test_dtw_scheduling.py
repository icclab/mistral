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

    # @mock.patch('mistral.services.triggers.validate_cron_trigger_input')
    # def test_create_cron_trigger_with_pattern_and_first_time(self,
    #                                                          validate_mock):
    #     cfg.CONF.set_default('auth_enable', False, group='pecan')

    #     wf = workflows.create_workflows(WORKFLOW_LIST)[0]

    #     # Make the first_time 1 sec later than current time, in order to make
    #     # it executed by next cron-trigger task.
    #     first_time = datetime.datetime.now() + datetime.timedelta(0, 1)

    #     # Creates a cron-trigger with pattern and first time, ensure the
    #     # cron-trigger can be executed more than once, and cron-trigger will
    #     # not be deleted.
    #     trigger_name = 'trigger-%s' % utils.generate_unicode_uuid()

    #     cron_trigger = triggers.create_cron_trigger(
    #         trigger_name,
    #         wf.name,
    #         {},
    #         {},
    #         '*/1 * * * *',
    #         first_time,
    #         None,
    #         None
    #     )

    #     self.assertEqual(
    #         first_time,
    #         cron_trigger.next_execution_time
    #     )

    #     periodic.MistralPeriodicTasks(cfg.CONF).process_cron_triggers_v2(None)

    #     next_time = triggers.get_next_execution_time(
    #         cron_trigger.pattern,
    #         cron_trigger.next_execution_time
    #     )

    #     cron_trigger_db = db_api.get_cron_trigger(trigger_name)

    #     self.assertIsNotNone(cron_trigger_db)
    #     self.assertEqual(
    #         next_time,
    #         cron_trigger_db.next_execution_time
    #     )
