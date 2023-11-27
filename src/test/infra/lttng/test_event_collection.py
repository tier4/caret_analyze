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


from caret_analyze.infra.lttng.lttng import EventCollection, IterableEvents

import pytest


@pytest.fixture
def set_pickle_collection_time_range(mocker):
    def _set_pickle_collection_time_range(time_min: int, time_max: int):
        pickle_collection_mock = mocker.Mock(spec=IterableEvents)
        mocker.patch('caret_analyze.infra.lttng.lttng.PickleEventCollection',
                     return_value=pickle_collection_mock)

        mocker.patch.object(
            pickle_collection_mock, 'time_range', return_value=(time_min, time_max))
    return _set_pickle_collection_time_range


@pytest.fixture
def set_ctf_collection_time_range(mocker):
    def _set_ctf_collection_time_range(time_min: int, time_max: int):
        ctf_collection_mock = mocker.Mock(spec=IterableEvents)
        mocker.patch('caret_analyze.infra.lttng.lttng.CtfEventCollection',
                     return_value=ctf_collection_mock)

        mocker.patch.object(
            ctf_collection_mock, 'time_range', return_value=(time_min, time_max))
    return _set_ctf_collection_time_range


@pytest.fixture
def set_trace_dir_exists(mocker):
    def _set_trace_dir_exists(exists: bool):
        mocker.patch('caret_analyze.infra.lttng.lttng.EventCollection._trace_dir_exists',
                     return_value=exists)
    return _set_trace_dir_exists


@pytest.fixture
def set_cache_exists(mocker):
    def _set_cache_exists(exists: bool):
        mocker.patch('caret_analyze.infra.lttng.lttng.EventCollection._cache_exists',
                     return_value=exists)
    return _set_cache_exists


class TestEventCollection:

    def test_force_conversion_case(
        self,
        caplog,
        set_pickle_collection_time_range,
        set_ctf_collection_time_range,
        set_trace_dir_exists,
        set_cache_exists
    ):
        set_trace_dir_exists(True)
        set_cache_exists(True)
        set_pickle_collection_time_range(0, 1)
        set_ctf_collection_time_range(0, 1)

        EventCollection('', True, store_cache=False)
        assert 'Converted to' in caplog.messages[0]

    def test_cache_not_exists_case(
        self,
        caplog,
        set_pickle_collection_time_range,
        set_ctf_collection_time_range,
        set_trace_dir_exists,
        set_cache_exists
    ):
        set_trace_dir_exists(True)
        set_cache_exists(False)
        set_pickle_collection_time_range(0, 1)
        set_ctf_collection_time_range(0, 1)

        EventCollection('', True, store_cache=False)
        assert 'Converted to' in caplog.messages[0]

    def test_valid_cache_exists_case(
        self,
        caplog,
        set_pickle_collection_time_range,
        set_ctf_collection_time_range,
        set_trace_dir_exists,
        set_cache_exists
    ):
        set_trace_dir_exists(True)
        set_cache_exists(True)
        set_pickle_collection_time_range(0, 1)
        set_ctf_collection_time_range(0, 1)

        EventCollection('', False, store_cache=False)
        assert 'Found converted file' in caplog.messages[0]

    def test_invalid_cache_exists_case(
        self,
        caplog,
        set_pickle_collection_time_range,
        set_ctf_collection_time_range,
        set_trace_dir_exists,
        set_cache_exists
    ):
        set_trace_dir_exists(True)
        set_cache_exists(True)
        set_pickle_collection_time_range(1, 2)
        set_ctf_collection_time_range(0, 1)

        EventCollection('', False, store_cache=False)
        assert 'Converted to' in caplog.messages[0]
