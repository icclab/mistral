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

from mistral.db.v2 import api as db_api
from mistral.services import security


def get_unscheduled_delay_tolerant_workload():
    """Return all workload that has not been initiated"""
    return db_api.get_delay_tolerant_workloads(**{'executed': False})


def create_delay_tolerant_workload(name, workflow_name, workflow_input,
                                   workflow_params=None, deadline=None,
                                   job_duration=None, workflow_id=None):
    # TODO(brunograz) - add params verification
    with db_api.transaction():
        wf_def = db_api.get_workflow_definition(
            workflow_id if workflow_id else workflow_name
        )

        values = {
            'name': name,
            'deadline': deadline,
            'job_duration': job_duration,
            'workflow_name': wf_def.name,
            'workflow_id': wf_def.id,
            'workflow_input': workflow_input or {},
            'workflow_params': workflow_params or {},
            'scope': 'private',
            'executed': False
        }

        security.add_trust_id(values)

        dtw = db_api.create_delay_tolerant_workload(values)

    return dtw
