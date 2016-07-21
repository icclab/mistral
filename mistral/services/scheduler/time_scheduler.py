#
# Copyright (c) 2016. Zuercher Hochschule fuer Angewandte Wissenschaften
# All Rights Reserved.
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
from apscheduler.schedulers.background import BackgroundScheduler
from dateutil.parser import parse as dateparse
import datetime
import six

from mistral import exceptions as exc


class TimeScheduler(object):
    """Workflow time scheduler.

    A workflow time scheduling mechanism which parses, validates and
    schedules a workflow for a specific point in the future. Cron tasks are
    also supported.
    """

    def __init__(self):
        self.scheduler = BackgroundScheduler()

    def schedule_task(self, name, workflow_name, workflow_input,
                      workflow_params=None, pattern=None, first_time=None,
                      count=None, start_time=None, workflow_id=None):
        if not start_time:
            start_time = datetime.datetime.now()

        if isinstance(first_time, six.string_types):
            try:
                first_time = dateparse(first_time)
            except ValueError as e:
                raise exc.InvalidModelException(e.message)

        # TODO(brunograz) - reimplementation of Mistral time scheduler
        # self.scheduler.add_job(task, 'date', run_date=date, args=args)
        # self.scheduler.start()
        # ...
