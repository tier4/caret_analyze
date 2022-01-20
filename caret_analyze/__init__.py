# Copyright 2021 Research Institute of Systems Planning, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from logging import DEBUG, Formatter, getLogger, StreamHandler, WARN

from .architecture import Architecture, check_procedure
from .common import Progress
from .infra.lttng.lttng import Lttng, LttngEventFilter
from .runtime.application import Application

__all__ = [
    'Application',
    'Architecture',
    'Lttng',
    'LttngEventFilter',
    'Progress',
    'check_procedure'
]


handler = StreamHandler()
handler.setLevel(WARN)

fmt = '%(levelname)-8s: %(asctime)s | %(message)s'
formatter = Formatter(
    fmt,
    datefmt='%Y-%m-%d %H:%M:%S')
handler.setFormatter(formatter)

logger = getLogger()
logger.setLevel(DEBUG)
logger.addHandler(handler)
