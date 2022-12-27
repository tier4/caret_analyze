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


from pathlib import Path

from .architecture import Architecture, check_procedure
from .common import init_logger, Progress
from .infra.lttng import Lttng, LttngEventFilter
from .runtime.application import Application

__all__ = [
    'Application',
    'Architecture',
    'Lttng',
    'LttngEventFilter',
    'Progress',
    'check_procedure'
]


"""
Configure root logger settings.

Note
----
Current implementation shows logs for all modules.
It should be possible to set the log level details
for each module in an external log configuration file.

"""

log_cfg_path = Path(__file__).parent/'log_config.yaml'
init_logger(str(log_cfg_path))
