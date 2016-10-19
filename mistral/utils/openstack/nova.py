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

from novaclient import client as nclient
from oslo_config import cfg

from mistral import context

CONF = cfg.CONF


def client():
    ctx = context.ctx()
    auth_url = ctx.auth_uri

    cl = nclient.Client(
        2,
        username=ctx.user_name,
        token=ctx.auth_token,
        tenant_id=ctx.project_id,
        auth_url=auth_url
    )

    cl.management_url = auth_url

    return cl


def get_flavor(flavor_id):
    return client().flavors.get(flavor_id).to_dict()
