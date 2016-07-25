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

from oslo_log import log as logging
from pecan import rest
from wsme import types as wtypes
import wsmeext.pecan as wsme_pecan

from mistral.api import access_control as acl
from mistral.api.controllers import resource
from mistral.api.controllers.v2 import types
from mistral import context
from mistral.db.v2 import api as db_api
from mistral.services import delay_tolerant_workload as dtw
from mistral.utils import rest_utils

LOG = logging.getLogger(__name__)
SCOPE_TYPES = wtypes.Enum(str, 'private', 'public')


class DelayTolerantWorkload(resource.Resource):
    """Delay Tolerant Workload resource"""

    id = wtypes.text
    name = wtypes.text
    workflow_name = wtypes.text
    workflow_id = wtypes.text
    workflow_input = types.jsontype
    workflow_params = types.jsontype

    scope = SCOPE_TYPES

    deadline = wtypes.text
    job_duration = wtypes.IntegerType(minimum=1)

    created_at = wtypes.text
    updated_at = wtypes.text

    @classmethod
    def sample(cls):
        return cls(id='123e4567-e89b-12d3-a456-426655440000',
                   name='DTW_test',
                   workflow_name='my_wf',
                   workflow_id='123e4567-e89b-12d3-a456-426655441111',
                   workflow_input={},
                   workflow_params={},
                   scope='private',
                   deadline='2016-07-23T00:00:00',
                   job_duration=4,
                   created_at='1970-01-01T00:00:00.000000',
                   updated_at='1970-01-01T00:00:00.000000')


class DelayTolerantWorkloads(resource.ResourceList):
    """A collection of delay tolerant workloads."""

    delay_tolerant_workloads = [DelayTolerantWorkload]

    def __init__(self, **kwargs):
        self._type = 'delay_tolerant_workloads'

        super(DelayTolerantWorkloads, self).__init__(**kwargs)

    @classmethod
    def sample(cls):
        return cls(delay_tolerant_workloads=[DelayTolerantWorkload.sample()])


class DelayTolerantWorkloadController(rest.RestController):
    @rest_utils.wrap_wsme_controller_exception
    @wsme_pecan.wsexpose(DelayTolerantWorkload, wtypes.text)
    def get(self, name):
        """Returns the named delay_tolerant_workload."""

        acl.enforce('delay_tolerant_workloads:get', context.ctx())
        LOG.info('Fetching Delay Tolerant Workload [name=%s]..' % name)

        db_model = db_api.get_delay_tolerant_workload(name)

        return DelayTolerantWorkload.from_dict(db_model.to_dict())

    @rest_utils.wrap_wsme_controller_exception
    @wsme_pecan.wsexpose(
        DelayTolerantWorkload,
        body=DelayTolerantWorkload,
        status_code=201
    )
    def post(self, delay_tolerant_workload):
        """Creates a new delay tolerant workload."""

        acl.enforce('delay_tolerant_workloads:create', context.ctx())
        LOG.info('Creating new delay tolerant workload: %s' %
                 delay_tolerant_workload)

        values = delay_tolerant_workload.to_dict()

        db_model = dtw.create_delay_tolerant_workload(
            values['name'],
            values.get('workflow_name'),
            values.get('workflow_input'),
            values.get('workflow_params'),
            values.get('deadline'),
            values.get('job_duration'),
            workflow_id=values.get('workflow_id')
        )

        return DelayTolerantWorkload.from_dict(db_model.to_dict())

    @rest_utils.wrap_wsme_controller_exception
    @wsme_pecan.wsexpose(None, wtypes.text, status_code=204)
    def delete(self, name):
        """Delete cron trigger."""
        acl.enforce('delay_tolerant_workloads:delete', context.ctx())
        LOG.info("Deleting delay tolerant workload [name=%s]" % name)

        db_api.delete_delay_tolerant_workload(name)

    @wsme_pecan.wsexpose(DelayTolerantWorkloads, wtypes.text,
                         wtypes.IntegerType(minimum=1), types.uuid, int,
                         types.uniquelist, types.list, types.uniquelist,
                         wtypes.text, wtypes.text, types.uuid, types.jsontype,
                         types.jsontype, SCOPE_TYPES,
                         wtypes.text, wtypes.text)
    def get_all(self, deadline=None, job_duration=None,
                marker=None, limit=None, sort_keys='created_at',
                sort_dirs='asc', fields='', name=None, workflow_name=None,
                workflow_id=None, workflow_input=None, workflow_params=None,
                scope=None, created_at=None, updated_at=None):
        """Return all cron triggers.

        :param deadline: Optional.
        :param job_duration: =Optional.
        :param marker: Optional. Pagination marker for large data sets.
        :param limit: Optional. Maximum number of resources to return in a
                      single result. Default value is None for backward
                      compatibility.
        :param sort_keys: Optional. Columns to sort results by.
                          Default: created_at, which is backward compatible.
        :param sort_dirs: Optional. Directions to sort corresponding to
                          sort_keys, "asc" or "desc" can be chosen.
                          Default: desc. The length of sort_dirs can be equal
                          or less than that of sort_keys.
        :param fields: Optional. A specified list of fields of the resource to
                       be returned. 'id' will be included automatically in
                       fields if it's provided, since it will be used when
                       constructing 'next' link.
        :param name: Optional. Keep only resources with a specific name.
        :param workflow_name: Optional. Keep only resources with a specific
                              workflow name.
        :param workflow_id: Optional. Keep only resources with a specific
                            workflow ID.
        :param workflow_input: Optional. Keep only resources with a specific
                               workflow input.
        :param workflow_params: Optional. Keep only resources with specific
                                workflow parameters.
        :param scope: Optional. Keep only resources with a specific scope.
        :param created_at: Optional. Keep only resources created at a specific
                           time and date.
        :param updated_at: Optional. Keep only resources with specific latest
                           update time and date.
        """
        acl.enforce('delay_tolerant_workloads:list', context.ctx())

        filters = rest_utils.filters_to_dict(
            created_at=created_at,
            name=name,
            updated_at=updated_at,
            workflow_name=workflow_name,
            workflow_id=workflow_id,
            workflow_input=workflow_input,
            workflow_params=workflow_params,
            scope=scope,
            job_duration=job_duration,
            deadline=deadline,
            )

        LOG.info("Fetching Delay tolerant workload. "
                 "marker=%s, limit=%s, sort_keys=%s, "
                 "sort_dirs=%s, filters=%s", marker, limit, sort_keys,
                 sort_dirs, filters)

        return rest_utils.get_all(
            DelayTolerantWorkloads,
            DelayTolerantWorkload,
            db_api.get_delay_tolerant_workloads,
            db_api.get_delay_tolerant_workload,
            resource_function=None,
            marker=marker,
            limit=limit,
            sort_keys=sort_keys,
            sort_dirs=sort_dirs,
            fields=fields,
            **filters
        )
