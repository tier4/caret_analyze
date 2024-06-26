# Copyright 2021 TIER IV, Inc.
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

from logging import CRITICAL, getLogger

import tempfile

from caret_analyze.common.logger_config import init_logger


class TestLoggerConfig:

    def test_empty(self, caplog) -> None:
        init_logger('not_found.yaml')
        assert 'Failed to load log config.' in caplog.messages[0]

    def test_empty_yaml(self, caplog) -> None:
        with tempfile.NamedTemporaryFile() as tp:
            init_logger(tp.name)
        assert 'Failed to load log config.' in caplog.messages[0]

    def test_load_config(self):
        with tempfile.NamedTemporaryFile(mode='w+t', encoding='utf-8', delete=False) as tf:
            tf.write(
                """
                version: 1
                loggers:
                    test:
                        level: CRITICAL
                disable_existing_loggers: false
                """
            )
        init_logger(tf.name)
        logger = getLogger('test')
        assert logger.level == CRITICAL
