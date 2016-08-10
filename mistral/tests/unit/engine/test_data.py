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

from datetime import datetime

ENERGY_PRICES = {
    "intra-day": {
        datetime.strptime("2016-07-06T00:00:00", "%Y-%m-%dT%H:%M:%S"): 24.0,
        datetime.strptime("2016-07-06T01:00:00", "%Y-%m-%dT%H:%M:%S"): 23,
        datetime.strptime("2016-07-06T02:00:00", "%Y-%m-%dT%H:%M:%S"): 17.4,
        datetime.strptime("2016-07-06T03:00:00", "%Y-%m-%dT%H:%M:%S"): 18.5,
        datetime.strptime("2016-07-06T04:00:00", "%Y-%m-%dT%H:%M:%S"): 20,
        datetime.strptime("2016-07-06T05:00:00", "%Y-%m-%dT%H:%M:%S"): 26,
        datetime.strptime("2016-07-06T06:00:00", "%Y-%m-%dT%H:%M:%S"): 28.2,
        datetime.strptime("2016-07-06T07:00:00", "%Y-%m-%dT%H:%M:%S"): 30.8,
        datetime.strptime("2016-07-06T08:00:00", "%Y-%m-%dT%H:%M:%S"): 32.3,
        datetime.strptime("2016-07-06T09:00:00", "%Y-%m-%dT%H:%M:%S"): 32,
        datetime.strptime("2016-07-06T10:00:00", "%Y-%m-%dT%H:%M:%S"): 39.6,
        datetime.strptime("2016-07-06T11:00:00", "%Y-%m-%dT%H:%M:%S"): 44.9,
        datetime.strptime("2016-07-06T12:00:00", "%Y-%m-%dT%H:%M:%S"): 32,
        datetime.strptime("2016-07-06T13:00:00", "%Y-%m-%dT%H:%M:%S"): 33,
        datetime.strptime("2016-07-06T14:00:00", "%Y-%m-%dT%H:%M:%S"): 31.8,
        datetime.strptime("2016-07-06T15:00:00", "%Y-%m-%dT%H:%M:%S"): 29.5,
        datetime.strptime("2016-07-06T16:00:00", "%Y-%m-%dT%H:%M:%S"): 30.5,
        datetime.strptime("2016-07-06T17:00:00", "%Y-%m-%dT%H:%M:%S"): 30.6,
        datetime.strptime("2016-07-06T18:00:00", "%Y-%m-%dT%H:%M:%S"): 31,
        datetime.strptime("2016-07-06T19:00:00", "%Y-%m-%dT%H:%M:%S"): 32,
        datetime.strptime("2016-07-06T20:00:00", "%Y-%m-%dT%H:%M:%S"): 36.2,
        datetime.strptime("2016-07-06T21:00:00", "%Y-%m-%dT%H:%M:%S"): 29.2,
        datetime.strptime("2016-07-06T22:00:00", "%Y-%m-%dT%H:%M:%S"): 34.4,
        datetime.strptime("2016-07-06T23:00:00", "%Y-%m-%dT%H:%M:%S"): 33.6
    },
    "day-ahead": {
        datetime.strptime("2016-07-07T00:00:00", "%Y-%m-%dT%H:%M:%S"): 30.4,
        datetime.strptime("2016-07-07T01:00:00", "%Y-%m-%dT%H:%M:%S"): 27.3,
        datetime.strptime("2016-07-07T02:00:00", "%Y-%m-%dT%H:%M:%S"): 27,
        datetime.strptime("2016-07-07T03:00:00", "%Y-%m-%dT%H:%M:%S"): 19,
        datetime.strptime("2016-07-07T04:00:00", "%Y-%m-%dT%H:%M:%S"): 20.5,
        datetime.strptime("2016-07-07T05:00:00", "%Y-%m-%dT%H:%M:%S"): 27.2,
        datetime.strptime("2016-07-07T06:00:00", "%Y-%m-%dT%H:%M:%S"): 30.4,
        datetime.strptime("2016-07-07T07:00:00", "%Y-%m-%dT%H:%M:%S"): 34.8,
        datetime.strptime("2016-07-07T08:00:00", "%Y-%m-%dT%H:%M:%S"): 36.2,
        datetime.strptime("2016-07-07T09:00:00", "%Y-%m-%dT%H:%M:%S"): 35.4,
        datetime.strptime("2016-07-07T10:00:00", "%Y-%m-%dT%H:%M:%S"): 36.5,
        datetime.strptime("2016-07-07T11:00:00", "%Y-%m-%dT%H:%M:%S"): 46,
        datetime.strptime("2016-07-07T12:00:00", "%Y-%m-%dT%H:%M:%S"): 42,
        datetime.strptime("2016-07-07T13:00:00", "%Y-%m-%dT%H:%M:%S"): 34,
        datetime.strptime("2016-07-07T14:00:00", "%Y-%m-%dT%H:%M:%S"): 43,
        datetime.strptime("2016-07-07T15:00:00", "%Y-%m-%dT%H:%M:%S"): 33.8,
        datetime.strptime("2016-07-07T16:00:00", "%Y-%m-%dT%H:%M:%S"): 34.55,
        datetime.strptime("2016-07-07T17:00:00", "%Y-%m-%dT%H:%M:%S"): 36,
        datetime.strptime("2016-07-07T18:00:00", "%Y-%m-%dT%H:%M:%S"): 37.6,
        datetime.strptime("2016-07-07T19:00:00", "%Y-%m-%dT%H:%M:%S"): 38.1,
        datetime.strptime("2016-07-07T20:00:00", "%Y-%m-%dT%H:%M:%S"): 33.5,
        datetime.strptime("2016-07-07T21:00:00", "%Y-%m-%dT%H:%M:%S"): 37.5,
        datetime.strptime("2016-07-07T22:00:00", "%Y-%m-%dT%H:%M:%S"): 37,
        datetime.strptime("2016-07-07T23:00:00", "%Y-%m-%dT%H:%M:%S"): 35
    }
}
