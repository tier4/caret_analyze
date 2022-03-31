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
from caret_analyze.record import Column, RecordFactory, RecordsFactory
from caret_analyze.record.pandas_extensions import PandasExtensions as pde


import pandas as pd
import pytest


class TestPandasExtensions:

    def test_to_records(self):
        df = pd.DataFrame.from_dict(
            [
                {'a': 1, 'b': None},
                {'a': 2},
            ],
            dtype='Int64'
        )

        records = pde.to_records(df)

        expect = RecordsFactory.create_instance(
            [
                RecordFactory.create_instance({'a': 1}),
                RecordFactory.create_instance({'a': 2})
            ],
            [
                Column('a'),
                Column('b')
            ]
        )

        assert records.equals(expect)

    def test_to_records_invalid_type(self):
        df = pd.DataFrame.from_dict(
            [
                {'a': 1, 'b': None},
                {'a': 2},
            ],
            dtype='int64'
        )

        with pytest.raises(InvalidArgumentError):
            pde.to_records(df)

    def test_merge_sequencial(self):
        df_left = pd.DataFrame.from_dict(
            [
                {'stamp': 0},
                {'stamp': 3},
                {'stamp': 4},
                {'stamp': 5},
                {'stamp': 8},
            ],
            dtype='Int64'
        )

        df_right = pd.DataFrame.from_dict(
            [
                {'sub_stamp': 1},
                {'sub_stamp': 6},
                {'sub_stamp': 7},
            ],
            dtype='Int64'
        )

        merged = pde.merge_sequencial(
            left_df=df_left,
            right_df=df_right,
            left_stamp_key='stamp',
            right_stamp_key='sub_stamp',
            join_left_key=None,
            join_right_key=None,
            how='outer'
        )

        expect = pd.DataFrame.from_dict(
            [
                {'stamp': 0, 'sub_stamp': 1},
                {'stamp': 3},
                {'stamp': 4},
                {'stamp': 5, 'sub_stamp': 6},
                {'sub_stamp': 7},
                {'stamp': 8},
            ],
            dtype='object'
        )

        assert merged.equals(expect)
