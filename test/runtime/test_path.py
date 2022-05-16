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
from caret_analyze.record import Record, Records
from caret_analyze.record.interface import RecordsInterface
from caret_analyze.runtime.communication import Communication
from caret_analyze.runtime.node_path import NodePath
from caret_analyze.runtime.path import ColumnMerger, Path, RecordsMerged
from caret_analyze.value_objects import PathStructValue

import pytest
from pytest_mock import MockerFixture


class TestPath:

    def test_empty(self, mocker: MockerFixture):
        column_merger_mock = mocker.Mock(spec=ColumnMerger)
        mocker.patch('caret_analyze.runtime.path.ColumnMerger',
                     return_value=column_merger_mock)

        records_merged_mock = mocker.Mock(spec=RecordsMerged)
        mocker.patch('caret_analyze.runtime.path.RecordsMerged',
                     return_value=records_merged_mock)

        records_mock = mocker.Mock(spec=RecordsInterface)
        mocker.patch.object(records_mock, 'clone', return_value=records_mock)
        mocker.patch.object(records_mock, 'columns', [])

        mocker.patch.object(column_merger_mock, 'column_names', [])
        mocker.patch.object(records_merged_mock, 'data', records_mock)

        path_info_mock = mocker.Mock(spec=PathStructValue)
        mocker.patch.object(path_info_mock, 'path_name', 'name')
        path = Path(path_info_mock, [], None)

        assert path.path_name == 'name'
        assert path.column_names == []
        assert path.communications == []
        assert path.node_paths == []

        records = path.to_records()
        assert records == records_mock
        assert path.column_names == []

    def test_str(self, mocker: MockerFixture):

        node_mock_0 = mocker.Mock(spec=NodePath)
        node_mock_1 = mocker.Mock(spec=NodePath)
        comm_mock_0 = mocker.Mock(spec=Communication)
        comm_mock_1 = mocker.Mock(spec=Communication)

        mocker.patch.object(node_mock_0, 'node_name', 'node0')
        mocker.patch.object(node_mock_1, 'node_name', 'node1')

        path_info_mock = mocker.Mock(spec=PathStructValue)
        mocker.patch.object(path_info_mock, 'path_name', 'name')
        path = Path(path_info_mock, [node_mock_0,
                    comm_mock_0, node_mock_1, comm_mock_1], None)
        assert str(path) == '\n'.join(['node0', 'node1'])

    def test_validate(self, mocker: MockerFixture):
        node_mock_0 = mocker.Mock(spec=NodePath)
        node_mock_1 = mocker.Mock(spec=NodePath)

        path_info_mock = mocker.Mock(spec=PathStructValue)
        with pytest.raises(InvalidArgumentError):
            Path(path_info_mock, [node_mock_0, node_mock_1], None)


class TestColumnMerged:

    def test_empty(self):
        merged = ColumnMerger()
        assert merged.column_names == []

    def test_column_names(self, mocker: MockerFixture):
        path_mock = mocker.Mock(spec=RecordsInterface)
        mocker.patch.object(path_mock, 'columns',
                            ['cb_start', 'xxx', 'pub'])

        comm_mock = mocker.Mock(spec=RecordsInterface)
        mocker.patch.object(comm_mock, 'columns', [
                            'pub', 'write', 'read', 'cb_start'])

        merger = ColumnMerger()
        merger.append_columns(path_mock)
        merger.append_columns(comm_mock)
        merger.append_columns(path_mock)

        assert merger.column_names == [
            'cb_start/0', 'xxx/0', 'pub/0',
            'write/0', 'read/0',
            'cb_start/1', 'xxx/1', 'pub/1'
        ]

    def test_rename_rule(self, mocker: MockerFixture):
        path_mock = mocker.Mock(spec=RecordsInterface)
        mocker.patch.object(path_mock, 'columns',
                            ['cb_start', 'xxx', 'pub'])

        comm_mock = mocker.Mock(spec=RecordsInterface)
        mocker.patch.object(comm_mock, 'columns', [
                            'pub', 'write', 'read', 'cb_start'])

        merger = ColumnMerger()
        rule = merger.append_columns_and_return_rename_rule(path_mock)
        assert rule == {
            'cb_start': 'cb_start/0',
            'xxx': 'xxx/0',
            'pub': 'pub/0',
        }

        rule = merger.append_columns_and_return_rename_rule(comm_mock)
        assert rule == {
            'pub': 'pub/0',
            'write': 'write/0',
            'read': 'read/0',
            'cb_start': 'cb_start/1',
        }

        rule = merger.append_columns_and_return_rename_rule(path_mock)
        assert rule == {
            'cb_start': 'cb_start/1',
            'xxx': 'xxx/1',
            'pub': 'pub/1',
        }


class TestRecordsMerged:

    def test_empty(self):
        with pytest.raises(InvalidArgumentError):
            RecordsMerged([])

    def test_merge_two_records(self, mocker: MockerFixture):
        node_path = mocker.Mock(spec=NodePath)
        mocker.patch.object(
            node_path, 'to_records',
            return_value=Records(
                [
                    Record({
                        'callback_start': 0, 'xxx': 1, 'pub': 2
                    }),
                ],
                ['callback_start', 'xxx', 'pub']
            )
        )

        comm_path = mocker.Mock(spec=Communication)
        mocker.patch.object(
            comm_path, 'to_records',
            return_value=Records(
                [
                    Record({'pub': 2, 'write': 4,
                            'read': 5, 'callback_start': 6}),
                ],
                ['pub', 'write', 'read', 'callback_start']
            )
        )

        # cb_mock = mocker.Mock(spec=Records)
        # comm_mock = mocker.Mock(spec=PathElement)

        # mocker.patch.object(cb_mock, 'to_records', side_effect=lambda: cb_records.clone())
        # mocker.patch.object(comm_mock, 'to_records', side_effect=lambda: comm_records.clone())

        merger_mock = mocker.Mock(spec=ColumnMerger)
        mocker.patch('caret_analyze.runtime.path.ColumnMerger',
                     return_value=merger_mock)

        def append_columns_and_return_rename_rule(records):
            if merger_mock.append_columns_and_return_rename_rule.call_count == 1:
                return {
                    'callback_start': 'callback_start/0', 'xxx': 'xxx/0', 'pub': 'pub/0'
                }
            if merger_mock.append_columns_and_return_rename_rule.call_count == 2:
                return {
                    'pub': 'pub/0',
                    'write': 'write/0',
                    'read': 'read/0',
                    'callback_start': 'callback_start/1'
                }
        mocker.patch.object(
            merger_mock, 'append_columns_and_return_rename_rule',
            side_effect=append_columns_and_return_rename_rule)

        merged = RecordsMerged([node_path, comm_path])
        records = merged.data
        expected = Records(
            [
                Record({
                    'callback_start/0': 0, 'xxx/0': 1, 'pub/0': 2,
                    'write/0': 4, 'read/0': 5, 'callback_start/1': 6
                }),
            ],
            ['callback_start/0', 'xxx/0', 'pub/0',
                'write/0', 'read/0', 'callback_start/1']
        )

        assert records.equals(expected)

    def test_loop_case(self, mocker: MockerFixture):
        cb_records = Records(
            [
                Record(
                    {'callback_start': 0, 'xxx': 1, 'pub': 2}),
                Record(
                    {'callback_start': 6, 'xxx': 7, 'pub': 8}),
                Record({'callback_start': 12,
                        'xxx': 13, 'pub': 14}),
            ],
            ['callback_start', 'xxx', 'pub']
        )
        node_path_0 = mocker.Mock(spec=NodePath)
        mocker.patch.object(
            node_path_0, 'to_records',
            return_value=cb_records.clone()
        )
        node_path_1 = mocker.Mock(spec=NodePath)
        mocker.patch.object(
            node_path_1, 'to_records',
            return_value=cb_records.clone()
        )

        comm_path = mocker.Mock(spec=Communication)
        mocker.patch.object(
            comm_path, 'to_records',
            return_value=Records(
                [
                    Record({'pub': 2, 'write': 4,
                            'read': 5, 'callback_start': 6}),
                    Record({'pub': 8, 'write': 10,
                            'read': 11, 'callback_start': 12}),
                ],
                ['pub', 'write', 'read', 'callback_start']
            )
        )

        def append_columns_and_return_rename_rule(records):
            if merger_mock.append_columns_and_return_rename_rule.call_count == 1:
                return {
                    'callback_start': 'callback_start/0', 'xxx': 'xxx/0', 'pub': 'pub/0'
                }
            if merger_mock.append_columns_and_return_rename_rule.call_count == 2:
                return {
                    'pub': 'pub/0',
                    'write': 'write/0',
                    'read': 'read/0',
                    'callback_start': 'callback_start/1'
                }
            if merger_mock.append_columns_and_return_rename_rule.call_count == 3:
                return {
                    'callback_start': 'callback_start/1',
                    'xxx': 'xxx/1',
                    'pub': 'pub/1'
                }

        merger_mock = mocker.Mock(spec=ColumnMerger)
        mocker.patch('caret_analyze.runtime.path.ColumnMerger',
                     return_value=merger_mock)
        mocker.patch.object(
            merger_mock, 'append_columns_and_return_rename_rule',
            side_effect=append_columns_and_return_rename_rule)

        merged = RecordsMerged([node_path_0, comm_path, node_path_1])
        records = merged.data
        expected = Records(
            [
                Record({
                    'callback_start/0': 0, 'xxx/0': 1, 'pub/0': 2,
                    'write/0': 4, 'read/0': 5,
                    'callback_start/1': 6, 'xxx/1': 7, 'pub/1': 8
                }),
                Record({
                    'callback_start/0': 6, 'xxx/0': 7, 'pub/0': 8,
                    'write/0': 10, 'read/0': 11,
                    'callback_start/1': 12, 'xxx/1': 13, 'pub/1': 14
                }),
                Record({
                    'callback_start/0': 12, 'xxx/0': 13, 'pub/0': 14
                }),
            ],
            [
                'callback_start/0', 'xxx/0', 'pub/0',
                'write/0', 'read/0',
                'callback_start/1', 'xxx/1', 'pub/1'
            ]
        )
        assert records.equals(expected)
