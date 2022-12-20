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


from logging import config, DEBUG, Formatter, getLogger, StreamHandler, WARN

import yaml


def init_logger(cfg_path: str):
    try:
        with open(cfg_path) as f:
            conf_dict = yaml.safe_load(f.read())
            config.dictConfig(conf_dict)

    except Exception as e:
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

        logger.warning('Failed to load log config.')
        logger.warning(e)
