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
from caret_analyze.record import ColumnValue, Record, Records, RecordsFactory
from caret_analyze.runtime.communication import Communication
from caret_analyze.runtime.node_path import NodePath
from caret_analyze.runtime.path import Path, RecordsMerged
from caret_analyze.value_objects import PathStructValue

import pytest


class TestPath:

    @pytest.fixture
    def create_path(self, mocker):
        def _create(path_info, child, callbacks, merged_records):
            records_merged = mocker.Mock(spec=RecordsMerged)
            mocker.patch.object(records_merged, 'data', merged_records)
            mocker.patch('caret_analyze.runtime.path.RecordsMerged',
                         return_value=records_merged)
            path = Path(path_info, child, callbacks)
            return path
        return _create

    def test_empty(self, mocker, create_path):
        merged_records = RecordsFactory.create_instance(
            None, [
                ColumnValue('column')
            ])

        path_info_mock = mocker.Mock(spec=PathStructValue)
        mocker.patch.object(path_info_mock, 'path_name', 'name')
        path = create_path(path_info_mock, [], [], merged_records)

        assert path.path_name == 'name'
        assert path.column_names == ['column']
        assert path.communications == []
        assert path.node_paths == []

        records = path.to_records()
        assert records.equals(merged_records)
        assert path.column_names == ['column']

    def test_str(self, mocker):

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

    def test_validate(self, mocker):
        node_mock_0 = mocker.Mock(spec=NodePath)
        node_mock_1 = mocker.Mock(spec=NodePath)

        path_info_mock = mocker.Mock(spec=PathStructValue)
        with pytest.raises(InvalidArgumentError):
            Path(path_info_mock, [node_mock_0, node_mock_1], None)


class TestRecordsMerged:

    def test_empty(self):
        with pytest.raises(InvalidArgumentError):
            RecordsMerged([])

    def test_merge_two_records(self, mocker):
        node_path = mocker.Mock(spec=NodePath)
        mocker.patch.object(
            node_path, 'to_records',
            return_value=Records(
                [
                    Record({
                        'callback_start': 0, 'xxx': 1, 'pub': 2
                    }),
                ],
                [
                    ColumnValue('callback_start'),
                    ColumnValue('xxx'),
                    ColumnValue('pub')
                ]
            )
        )

        comm_path = mocker.Mock(spec=Communication)
        mocker.patch.object(
            comm_path, 'to_records',
            return_value=Records(
                [
                    Record({'pub': 2, 'write': 4, 'read': 5, 'callback_start': 6}),
                ],
                [
                    ColumnValue('pub'),
                    ColumnValue('write'),
                    ColumnValue('read'),
                    ColumnValue('callback_start')
                ]
            )
        )

        merged = RecordsMerged([node_path, comm_path])
        records = merged.data
        expected = Records(
            [
                Record({
                    'callback_start/0': 0, 'xxx/0': 1, 'pub/0': 2,
                    'write/1': 4, 'read/1': 5, 'callback_start/1': 6
                }),
            ],
            [
                ColumnValue('callback_start', suffix=['0']),
                ColumnValue('xxx', suffix=['0']),
                ColumnValue('pub', suffix=['0']),
                ColumnValue('write', suffix=['1']),
                ColumnValue('read', suffix=['1']),
                ColumnValue('callback_start', suffix=['1'])
            ]
        )

        assert records.equals(expected)

    def test_loop_case(self, mocker):
        cb_records = Records(
            [
                Record(
                    {'callback_start': 0, 'xxx': 1, 'pub': 2}),
                Record(
                    {'callback_start': 6, 'xxx': 7, 'pub': 8}),
                Record({'callback_start': 12,
                        'xxx': 13, 'pub': 14}),
            ],
            [
                ColumnValue('callback_start'),
                ColumnValue('xxx'),
                ColumnValue('pub')
            ]
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
                [
                    ColumnValue('pub'),
                    ColumnValue('write'),
                    ColumnValue('read'),
                    ColumnValue('callback_start')
                ]
            )
        )

        merged = RecordsMerged([node_path_0, comm_path, node_path_1])
        records = merged.data
        expected = Records(
            [
                Record({
                    'callback_start/0': 0, 'xxx/0': 1, 'pub/0': 2,
                    'write/1': 4, 'read/1': 5,
                    'callback_start/1': 6, 'xxx/2': 7, 'pub/2': 8
                }),
                Record({
                    'callback_start/0': 6, 'xxx/0': 7, 'pub/0': 8,
                    'write/1': 10, 'read/1': 11,
                    'callback_start/1': 12, 'xxx/2': 13, 'pub/2': 14
                }),
                Record({
                    'callback_start/0': 12, 'xxx/0': 13, 'pub/0': 14
                }),
            ],
            [
                ColumnValue('callback_start', suffix=['0']),
                ColumnValue('xxx', suffix=['0']),
                ColumnValue('pub', suffix=['0']),
                ColumnValue('write', suffix=['1']),
                ColumnValue('read', suffix=['1']),
                ColumnValue('callback_start', suffix=['1']),
                ColumnValue('xxx', suffix=['2']),
                ColumnValue('pub', suffix=['2'])
            ]
        )
        assert records.equals(expected)
