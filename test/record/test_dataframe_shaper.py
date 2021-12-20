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

from caret_analyze.record.data_frame_shaper import Clip, Strip

import pandas as pd


class TestStrip:

    def test_exec_lstrip(self):
        df = pd.DataFrame.from_dict(
            [
                {'t_ns': 1*1.0e9},
                {'t_ns': 2*1.0e9},
                {'t_ns': 3*1.0e9},
                {'t_ns': 4*1.0e9},
            ]
        )
        strip = Strip(lstrip_s=1, rstrip_s=0)
        df_stripped = strip.execute(df)
        expect_df = pd.DataFrame.from_dict(
            [
                {'t_ns': 2*1.0e9},
                {'t_ns': 3*1.0e9},
                {'t_ns': 4*1.0e9},
            ]
        )
        assert df_stripped.equals(expect_df)

    def test_exec_rstrip(self):
        df = pd.DataFrame.from_dict(
            [
                {'t_ns': 1*1.0e9},
                {'t_ns': 2*1.0e9},
                {'t_ns': 3*1.0e9},
                {'t_ns': 4*1.0e9},
            ]
        )
        strip = Strip(lstrip_s=0, rstrip_s=1)
        df_stripped = strip.execute(df)
        expect_df = pd.DataFrame.from_dict(
            [
                {'t_ns': 1*1.0e9},
                {'t_ns': 2*1.0e9},
                {'t_ns': 3*1.0e9},
            ]
        )
        assert df_stripped.equals(expect_df)

    def test_exec_empty_columns(self):
        df = pd.DataFrame()
        strip = Strip(lstrip_s=1, rstrip_s=1)
        df_stripped = strip.execute(df)

        expect_df = pd.DataFrame.from_dict(
            []
        )
        assert df_stripped.equals(expect_df)

    def test_exec_empty_row(self):
        df = pd.DataFrame([], columns=['a'])
        strip = Strip(lstrip_s=1, rstrip_s=1)
        df_stripped = strip.execute(df)

        expect_df = pd.DataFrame([], columns=['a'])
        assert df_stripped.equals(expect_df)


class TestClip:

    def test_exec_lstrip(self):
        df = pd.DataFrame.from_dict(
            [
                {'t_ns': 1*1.0e9},
                {'t_ns': 2*1.0e9},
                {'t_ns': 3*1.0e9},
                {'t_ns': 4*1.0e9},
            ]
        )
        clip = Clip(ltrim_ns=2*1.0e9, rtrim_ns=4*1.0e9)
        df_clipped = clip.execute(df)
        expect_df = pd.DataFrame.from_dict(
            [
                {'t_ns': 2*1.0e9},
                {'t_ns': 3*1.0e9},
                {'t_ns': 4*1.0e9},
            ]
        )
        assert df_clipped.equals(expect_df)

    def test_exec_rstrip(self):
        df = pd.DataFrame.from_dict(
            [
                {'t_ns': 1*1.0e9},
                {'t_ns': 2*1.0e9},
                {'t_ns': 3*1.0e9},
                {'t_ns': 4*1.0e9},
            ]
        )
        clip = Clip(ltrim_ns=0, rtrim_ns=3*1.0e9)
        df_clipped = clip.execute(df)
        expect_df = pd.DataFrame.from_dict(
            [
                {'t_ns': 1*1.0e9},
                {'t_ns': 2*1.0e9},
                {'t_ns': 3*1.0e9},
            ]
        )
        assert df_clipped.equals(expect_df)

    def test_exec_empty_columns(self):
        df = pd.DataFrame()
        clip = Clip(ltrim_ns=0, rtrim_ns=0)
        df_stripped = clip.execute(df)

        expect_df = pd.DataFrame.from_dict(
            []
        )
        assert df_stripped.equals(expect_df)

    def test_exec_empty_row(self):
        df = pd.DataFrame([], columns=['a'])
        clip = Clip(ltrim_ns=0, rtrim_ns=0)
        df_clipped = clip.execute(df)

        expect_df = pd.DataFrame([], columns=['a'])
        assert df_clipped.equals(expect_df)
