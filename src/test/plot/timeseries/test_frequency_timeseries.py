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

from caret_analyze.exceptions import InvalidArgumentError
from caret_analyze.plot.timeseries.frequency_timeseries import FrequencyTimeSeries
from caret_analyze.runtime.callback import CallbackBase

import numpy as np

import pandas as pd

import pytest


class TestFrequencyTimeSeries:

    def test_get_timestamp_range_normal(self, mocker):
        object_mock0 = mocker.Mock(spec=CallbackBase)
        mocker.patch.object(
            object_mock0, 'to_dataframe',
            return_value=pd.DataFrame(data={
                'first': [1, 2],
                'last': [2, 3]
            })
        )
        object_mock1 = mocker.Mock(spec=CallbackBase)
        mocker.patch.object(
            object_mock1, 'to_dataframe',
            return_value=pd.DataFrame(data={
                'first': [4, 5],
                'last': [5, 6]
            })
        )
        min_ts, max_ts = FrequencyTimeSeries._get_timestamp_range(
            [object_mock0, object_mock1])
        assert min_ts == 1
        assert max_ts == 5

    def test_get_timestamp_range_empty_input(self):
        with pytest.raises(InvalidArgumentError) as e:
            FrequencyTimeSeries._get_timestamp_range([])

        assert 'timestamp' in str(e.value)

    def test_get_timestamp_range_exist_empty_df(self, mocker):
        object_mock0 = mocker.Mock(spec=CallbackBase)
        mocker.patch.object(
            object_mock0, 'to_dataframe',
            return_value=pd.DataFrame(data={
                'first': [1, 2],
                'last': [2, 3]
            })
        )
        object_mock1 = mocker.Mock(spec=CallbackBase)
        mocker.patch.object(
            object_mock1, 'to_dataframe',
            return_value=pd.DataFrame()
        )
        min_ts, max_ts = FrequencyTimeSeries._get_timestamp_range(
            [object_mock0, object_mock1])
        assert min_ts == 1
        assert max_ts == 2

    def test_get_timestamp_range_df_include_nan(self, mocker):
        object_mock0 = mocker.Mock(spec=CallbackBase)
        mocker.patch.object(
            object_mock0, 'to_dataframe',
            return_value=pd.DataFrame(data={
                'first': [1, 2],
                'last': [2, 3]
            })
        )
        object_mock1 = mocker.Mock(spec=CallbackBase)
        mocker.patch.object(
            object_mock1, 'to_dataframe',
            return_value=pd.DataFrame(data={
                'first': [np.nan, 5],
                'last': [5, 6]
            }
            )
        )
        min_ts, max_ts = FrequencyTimeSeries._get_timestamp_range(
            [object_mock0, object_mock1])
        assert min_ts == 1
        assert max_ts == 5

    def test_get_timestamp_range_df_only_nan(self, mocker):
        object_mock0 = mocker.Mock(spec=CallbackBase)
        mocker.patch.object(
            object_mock0, 'to_dataframe',
            return_value=pd.DataFrame(data={
                'first': [np.nan, np.nan],
                'last': [np.nan, np.nan]
            })
        )
        object_mock1 = mocker.Mock(spec=CallbackBase)
        mocker.patch.object(
            object_mock1, 'to_dataframe',
            return_value=pd.DataFrame(data={
                'first': [np.nan, np.nan],
                'last': [np.nan, np.nan]
            }
            )
        )
        with pytest.raises(InvalidArgumentError) as e:
            FrequencyTimeSeries._get_timestamp_range(
                [object_mock0, object_mock1])

        assert 'timestamp' in str(e.value)

    def test_get_timestamp_range_df_include_none(self, mocker):
        object_mock0 = mocker.Mock(spec=CallbackBase)
        mocker.patch.object(
            object_mock0, 'to_dataframe',
            return_value=pd.DataFrame(data={
                'first': [1, 2],
                'last': [2, 3]
            })
        )
        object_mock1 = mocker.Mock(spec=CallbackBase)
        mocker.patch.object(
            object_mock1, 'to_dataframe',
            return_value=pd.DataFrame(data={
                'first': [None, 5],
                'last': [5, 6]
            }
            )
        )
        min_ts, max_ts = FrequencyTimeSeries._get_timestamp_range(
            [object_mock0, object_mock1])
        assert min_ts == 1
        assert max_ts == 5

    def test_get_timestamp_range_df_only_none(self, mocker):
        object_mock0 = mocker.Mock(spec=CallbackBase)
        mocker.patch.object(
            object_mock0, 'to_dataframe',
            return_value=pd.DataFrame(data={
                'first': [None, None],
                'last': [None, None]
            })
        )
        object_mock1 = mocker.Mock(spec=CallbackBase)
        mocker.patch.object(
            object_mock1, 'to_dataframe',
            return_value=pd.DataFrame(data={
                'first': [None, None],
                'last': [None, None]
            }
            )
        )
        with pytest.raises(InvalidArgumentError) as e:
            FrequencyTimeSeries._get_timestamp_range(
                [object_mock0, object_mock1])

        assert 'timestamp' in str(e.value)

    def test_get_timestamp_range_len_timestamp_is_0(self, mocker):
        object_mock0 = mocker.Mock(spec=CallbackBase)
        mocker.patch.object(
            object_mock0, 'to_dataframe',
            return_value=pd.DataFrame()
        )
        object_mock1 = mocker.Mock(spec=CallbackBase)
        mocker.patch.object(
            object_mock1, 'to_dataframe',
            return_value=pd.DataFrame()
        )
        with pytest.raises(InvalidArgumentError) as e:
            FrequencyTimeSeries._get_timestamp_range(
                [object_mock0, object_mock1])

        assert 'timestamp' in str(e.value)
