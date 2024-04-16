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

from caret_analyze.exceptions import InvalidArgumentError
from caret_analyze.infra import RecordsProvider
from caret_analyze.record.column import ColumnValue
from caret_analyze.record.interface import RecordsInterface
from caret_analyze.record.record_cpp_impl import RecordCppImpl, RecordsCppImpl
from caret_analyze.runtime.callback import CallbackBase
from caret_analyze.runtime.communication import Communication
from caret_analyze.runtime.node_path import NodePath
from caret_analyze.runtime.path import ColumnMerger, Path, RecordsMerged
from caret_analyze.value_objects import NodePathStructValue, PathStructValue

import pytest


class TestPath:

    def test_empty(self, mocker):
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

    def test_include_first_callback(self, mocker):
        path_struct = mocker.Mock(spec=PathStructValue)
        child = mocker.Mock(spec=NodePath)
        callbacks = mocker.Mock(spec=CallbackBase)
        path = Path(
            path_struct,
            [child],
            [callbacks]
        )
        assert not path.include_first_callback
        path.include_first_callback = True
        assert path.include_first_callback
        path.include_first_callback = False
        assert not path.include_first_callback

    def test_include_last_callback(self, mocker):
        path_struct = mocker.Mock(spec=PathStructValue)
        child = mocker.Mock(spec=NodePath)
        callbacks = mocker.Mock(spec=CallbackBase)
        path = Path(
            path_struct,
            [child],
            [callbacks]
        )
        assert not path.include_last_callback
        path.include_last_callback = True
        assert path.include_last_callback
        path.include_last_callback = False
        assert not path.include_last_callback

    def test_cache(self, mocker):
        path_struct_value = mocker.Mock(spec=PathStructValue)
        records_provider = mocker.Mock(spec=RecordsProvider)
        node_path_value = mocker.Mock(spec=NodePathStructValue)
        node_path = NodePath(
            node_path_value,
            records_provider,
            subscription=None,
            publisher=None,
            callbacks=None
        )

        path = Path(
            path=path_struct_value,
            child=[node_path],
            callbacks=None
        )

        records_mock = mocker.Mock(spec=RecordsCppImpl)
        mocker.patch.object(path, '_to_records_core', return_value=records_mock)

        path.to_records()
        assert path._to_records_core.call_count == 1  # type: ignore
        path.to_records()
        assert path._to_records_core.call_count == 1  # type: ignore

        path.include_first_callback = True
        path.to_records()
        assert path._to_records_core.call_count == 2  # type: ignore
        path.to_records()
        assert path._to_records_core.call_count == 2  # type: ignore

        path.include_first_callback = False
        path.include_last_callback = True
        path.to_records()
        assert path._to_records_core.call_count == 3  # type: ignore
        path.to_records()
        assert path._to_records_core.call_count == 3  # type: ignore

        path.include_first_callback = True
        path.include_last_callback = True
        path.to_records()
        assert path._to_records_core.call_count == 4  # type: ignore
        path.to_records()
        assert path._to_records_core.call_count == 4  # type: ignore

        path.clear_cache()
        path.to_records()
        assert path._to_records_core.call_count == 5  # type: ignore


class TestColumnMerged:

    def test_empty(self):
        merged = ColumnMerger()
        assert merged.column_names == []

    def test_columns(self, mocker):
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

    def test_rename_rule(self, mocker):
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

    def test_merge_two_records(self, mocker):
        node_path = mocker.Mock(spec=NodePath)
        mocker.patch.object(
            node_path, 'to_records',
            return_value=RecordsCppImpl(
                [
                    RecordCppImpl({
                        'callback_start': 0, 'xxx': 1, 'pub': 2
                    }),
                ],
                [
                    ColumnValue('callback_start'),
                    ColumnValue('xxx'),
                    ColumnValue('pub'),
                ]
            )
        )

        comm_path = mocker.Mock(spec=Communication)
        mocker.patch.object(
            comm_path, 'to_records',
            return_value=RecordsCppImpl(
                [
                    RecordCppImpl({'pub': 2, 'write': 4,
                                   'read': 5, 'callback_start': 6}),
                ],
                [
                    ColumnValue('pub'),
                    ColumnValue('write'),
                    ColumnValue('read'),
                    ColumnValue('callback_start'),
                ]
            )
        )

        # cb_mock = mocker.Mock(spec=RecordsCppImpl)
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
        expected = RecordsCppImpl(
            [
                RecordCppImpl({
                    'callback_start/0': 0, 'xxx/0': 1, 'pub/0': 2,
                    'write/0': 4, 'read/0': 5, 'callback_start/1': 6
                }),
            ],
            [
                ColumnValue('callback_start/0'),
                ColumnValue('xxx/0'),
                ColumnValue('pub/0'),
                ColumnValue('write/0'),
                ColumnValue('read/0'),
                ColumnValue('callback_start/1'),
            ]
        )

        assert records.equals(expected)

    def test_merge_two_records_include_first_callback(self, mocker):
        node_path = mocker.Mock(spec=NodePath)
        mocker.patch.object(
            node_path, 'to_records',
            return_value=RecordsCppImpl(
                None
            )
        )
        mocker.patch.object(
            node_path, 'to_path_beginning_records',
            return_value=RecordsCppImpl(
                [
                    RecordCppImpl({
                        'callback_start': 0, 'fff': 1, 'pub': 2
                    }),
                ],
                [
                    ColumnValue('callback_start'),
                    ColumnValue('fff'),
                    ColumnValue('pub'),
                ]
            )
        )

        comm_path = mocker.Mock(spec=Communication)
        mocker.patch.object(
            comm_path, 'to_records',
            return_value=RecordsCppImpl(
                [
                    RecordCppImpl({'pub': 2, 'write': 4,
                                   'read': 5, 'callback_start': 6}),
                ],
                [
                    ColumnValue('pub'),
                    ColumnValue('write'),
                    ColumnValue('read'),
                    ColumnValue('callback_start'),
                ]
            )
        )

        merger_mock = mocker.Mock(spec=ColumnMerger)
        mocker.patch('caret_analyze.runtime.path.ColumnMerger',
                     return_value=merger_mock)

        rename_rules = [
            {
                'callback_start': 'callback_start/0', 'fff': 'fff/0', 'pub': 'pub/0'
            },
            {
                'pub': 'pub/0',
                'write': 'write/0',
                'read': 'read/0',
                'callback_start': 'callback_start/1'
            }
        ]
        mocker.patch.object(
            merger_mock, 'append_columns_and_return_rename_rule',
            side_effect=rename_rules)

        merged = RecordsMerged([node_path, comm_path], include_first_callback=True)
        records = merged.data
        expected = RecordsCppImpl(
            [
                RecordCppImpl({
                    'callback_start/0': 0, 'fff/0': 1, 'pub/0': 2,
                    'write/0': 4, 'read/0': 5, 'callback_start/1': 6
                }),
            ],
            [
                ColumnValue('callback_start/0'),
                ColumnValue('fff/0'),
                ColumnValue('pub/0'),
                ColumnValue('write/0'),
                ColumnValue('read/0'),
                ColumnValue('callback_start/1'),
            ]
        )

        assert records.equals(expected)

    def test_merge_two_records_include_last_callback(self, mocker):
        node_path = mocker.Mock(spec=NodePath)
        mocker.patch.object(
            node_path, 'to_records',
            return_value=RecordsCppImpl(
                None
            )
        )
        mocker.patch.object(
            node_path, 'to_path_end_records',
            return_value=RecordsCppImpl(
                [
                    RecordCppImpl({
                        'callback_start': 3, 'callback_end': 4
                    }),
                ],
                [
                    ColumnValue('callback_start'),
                    ColumnValue('callback_end'),
                ]
            )
        )

        comm_path = mocker.Mock(spec=Communication)
        mocker.patch.object(
            comm_path, 'to_records',
            return_value=RecordsCppImpl(
                [
                    RecordCppImpl({'pub': 0, 'write': 1,
                                   'read': 2, 'callback_start': 3}),
                ],
                [
                    ColumnValue('pub'),
                    ColumnValue('write'),
                    ColumnValue('read'),
                    ColumnValue('callback_start'),
                ]
            )
        )

        merger_mock = mocker.Mock(spec=ColumnMerger)
        mocker.patch('caret_analyze.runtime.path.ColumnMerger',
                     return_value=merger_mock)

        rename_rules = [
            {
                'pub': 'pub/0',
                'write': 'write/0',
                'read': 'read/0',
                'callback_start': 'callback_start/1'
            },
            {
                'callback_start': 'callback_start/1',
                'callback_end': 'callback_end/1',
            },
        ]
        mocker.patch.object(
            merger_mock, 'append_columns_and_return_rename_rule',
            side_effect=rename_rules)

        merged = RecordsMerged([comm_path, node_path], include_last_callback=True)
        records = merged.data
        expected = RecordsCppImpl(
            [
                RecordCppImpl({
                    'pub/0': 0, 'write/0': 1, 'read/0': 2,
                    'callback_start/1': 3, 'callback_end/1': 4
                }),
            ],
            [
                ColumnValue('pub/0'),
                ColumnValue('write/0'),
                ColumnValue('read/0'),
                ColumnValue('callback_start/1'),
                ColumnValue('callback_end/1')
            ]
        )

        assert records.equals(expected)

    def test_loop_case(self, mocker):
        cb_records = RecordsCppImpl(
            [
                RecordCppImpl(
                    {'callback_start': 0, 'xxx': 1, 'pub': 2}),
                RecordCppImpl(
                    {'callback_start': 6, 'xxx': 7, 'pub': 8}),
                RecordCppImpl({'callback_start': 12,
                               'xxx': 13, 'pub': 14}),
            ],
            [
                ColumnValue('callback_start'),
                ColumnValue('xxx'),
                ColumnValue('pub'),
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
            return_value=RecordsCppImpl(
                [
                    RecordCppImpl({'pub': 2, 'write': 4,
                                   'read': 5, 'callback_start': 6}),
                    RecordCppImpl({'pub': 8, 'write': 10,
                                   'read': 11, 'callback_start': 12}),
                ],
                [
                    ColumnValue('pub'),
                    ColumnValue('write'),
                    ColumnValue('read'),
                    ColumnValue('callback_start'),
                ]
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
        expected = RecordsCppImpl(
            [
                RecordCppImpl({
                    'callback_start/0': 0, 'xxx/0': 1, 'pub/0': 2,
                    'write/0': 4, 'read/0': 5,
                    'callback_start/1': 6, 'xxx/1': 7, 'pub/1': 8
                }),
                RecordCppImpl({
                    'callback_start/0': 6, 'xxx/0': 7, 'pub/0': 8,
                    'write/0': 10, 'read/0': 11,
                    'callback_start/1': 12, 'xxx/1': 13, 'pub/1': 14
                }),
                RecordCppImpl({
                    'callback_start/0': 12, 'xxx/0': 13, 'pub/0': 14
                }),
            ],
            [
                ColumnValue('callback_start/0'),
                ColumnValue('xxx/0'),
                ColumnValue('pub/0'),
                ColumnValue('write/0'),
                ColumnValue('read/0'),
                ColumnValue('callback_start/1'),
                ColumnValue('xxx/1'),
                ColumnValue('pub/1'),
            ]
        )
        assert records.equals(expected)
