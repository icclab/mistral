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


class EnergyMarketScheduler(object):
    """Energy market place scheduler.

    The scheduler receives price inputs from the energy spot market and reacts
    to low prices. A set of workflows tagged as 'Flexible' selected by the
    scheduler are then executed.
    This scheduler aims to reduce the total energy consumed of the cluster by
    scheduling work at times when the cluster in underutilized and/or the
    energy price is considerable low.
    """

    def __init__(self):
        self.avg_energy_price = 0

    def pre_execution(self):
        """Collect information from the cluster and energy price.

        :return: None
        """
        # Simple example.
        if self.avg_energy_price < self.get_energy_price():
            # TODO - Trigger execution of a set of workflows.
            pass
        pass

    def execute(self):
        pass

    def get_energy_price(self):
        # TODO(all) - Link this function with the energy price from the
            # energy spot market API.
        pass

    def event_triggered(self):
        pass
