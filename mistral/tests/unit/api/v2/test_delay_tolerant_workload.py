# -*- coding: utf-8 -*-
#
# Copyright (c) 2016. Zuercher Hochschule fuer Angewandte Wissenschaften
# All Rights Reserved.
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#

import copy
import json
import mock

from mistral.db.v2 import api as db_api
from mistral.db.v2.sqlalchemy import models
from mistral import exceptions as exc
from mistral.tests.unit.api import base

WF = models.WorkflowDefinition(
    spec={
        'version': '2.0',
        'name': 'my_wf',
        'tasks': {
            'task1': {
                'action': 'std.noop'
            }
        }
    }
)
WF.update({'id': '123e4567-e89b-12d3-a456-426655440000', 'name': 'my_wf'})

DTW = {
    'id': '123',
    'name': 'dtw_test',
    'deadline': '2016-07-22T00:00:00',
    'workflow_name': WF.name,
    'job_duration': 4,
    'workflow_id': '123e4567-e89b-12d3-a456-426655440000',
    'workflow_input': '{}',
    'workflow_params': '{}',
    'scope': 'private'
}

DTW_values = copy.deepcopy(DTW)
DTW_values['workflow_input'] = json.loads(
    DTW_values['workflow_input'])

DTW_values['workflow_params'] = json.loads(
    DTW_values['workflow_params'])

DTW_DB = models.DTWorkload()
DTW_DB.update(DTW_values)

MOCK_WF = mock.MagicMock(return_value=WF)
MOCK_DTW = mock.MagicMock(return_value=DTW_DB)
MOCK_DTWs = mock.MagicMock(return_value=[DTW_DB])
MOCK_DELETE = mock.MagicMock(return_value=None)
MOCK_EMPTY = mock.MagicMock(return_value=[])
MOCK_NOT_FOUND = mock.MagicMock(side_effect=exc.DBEntityNotFoundError())
MOCK_DUPLICATE = mock.MagicMock(side_effect=exc.DBDuplicateEntryError())


class TestDelayTolerantWorkloadController(base.APITest):
    @mock.patch.object(db_api, "get_delay_tolerant_workload", MOCK_DTW)
    def test_get(self):
        resp = self.app.get(
                '/v2/delay_tolerant_workload/dtw_test'
        )

        self.assertEqual(200, resp.status_int)
        self.assertDictEqual(DTW, resp.json)

    @mock.patch.object(db_api, "get_delay_tolerant_workload", MOCK_NOT_FOUND)
    def test_get_not_found(self):
        resp = self.app.get(
            '/v2/delay_tolerant_workload/delay_tolerant_workload',
            expect_errors=True
        )

        self.assertEqual(404, resp.status_int)

    @mock.patch.object(db_api, "get_workflow_definition", MOCK_WF)
    @mock.patch.object(db_api, "create_delay_tolerant_workload")
    def test_post(self, mock_mtd):
        mock_mtd.return_value = DTW_DB
        resp = self.app.post_json('/v2/delay_tolerant_workload', DTW)

        self.assertEqual(201, resp.status_int)
        self.assertDictEqual(DTW, resp.json)

        self.assertEqual(1, mock_mtd.call_count)

        values = mock_mtd.call_args[0][0]

        self.assertEqual('2016-07-22T00:00:00', values['deadline'])
        self.assertEqual(4, values['job_duration'])

    @mock.patch.object(db_api, "get_workflow_definition", MOCK_WF)
    @mock.patch.object(
            db_api,
            "create_delay_tolerant_workload",
            MOCK_DUPLICATE
    )
    def test_post_dup(self):
        resp = self.app.post_json(
            '/v2/delay_tolerant_workload', DTW, expect_errors=True
        )

        self.assertEqual(409, resp.status_int)

    @mock.patch.object(db_api, "get_workflow_definition", MOCK_WF)
    @mock.patch.object(
            db_api,
            "create_delay_tolerant_workload",
            MOCK_DUPLICATE
    )
    def test_post_same_wf_and_input(self):
        dtw = DTW.copy()
        dtw['name'] = 'some_trigger_name'

        resp = self.app.post_json(
            '/v2/delay_tolerant_workload', dtw, expect_errors=True
        )

        self.assertEqual(409, resp.status_int)

    @mock.patch.object(db_api, "get_delay_tolerant_workload", MOCK_DTW)
    @mock.patch.object(db_api, "delete_delay_tolerant_workload", MOCK_DELETE)
    def test_delete(self):
        resp = self.app.delete(
                '/v2/delay_tolerant_workload/delay_tolerant_workload'
        )

        self.assertEqual(204, resp.status_int)

    @mock.patch.object(
            db_api,
            "delete_delay_tolerant_workload",
            MOCK_NOT_FOUND
    )
    def test_delete_not_found(self):
        resp = self.app.delete(
            '/v2/delay_tolerant_workload/delay_tolerant_workload',
            expect_errors=True
        )

        self.assertEqual(404, resp.status_int)

    @mock.patch.object(db_api, "get_delay_tolerant_workload", MOCK_DTWs)
    def test_get_all(self):
        resp = self.app.get('/v2/delay_tolerant_workload')

        self.assertEqual(200, resp.status_int)

        self.assertEqual(1, len(resp.json['delay_tolerant_workload']))
        self.assertDictEqual(DTW, resp.json['delay_tolerant_workload'][0])

    @mock.patch.object(db_api, "get_delay_tolerant_workload", MOCK_EMPTY)
    def test_get_all_empty(self):
        resp = self.app.get('/v2/delay_tolerant_workload')

        self.assertEqual(200, resp.status_int)

        self.assertEqual(0, len(resp.json['delay_tolerant_workload']))
