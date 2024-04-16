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

from caret_analyze.infra import RecordsProvider
from caret_analyze.record.column import ColumnValue
from caret_analyze.record.record_cpp_impl import RecordCppImpl, RecordsCppImpl
from caret_analyze.runtime.node_path import NodePath
from caret_analyze.value_objects import NodePathStructValue


class TestNodePath:

    def test_beginning_path(self, mocker):
        records_provider = mocker.Mock(spec=RecordsProvider)
        node_path_value = mocker.Mock(spec=NodePathStructValue)
        node_path = NodePath(
            node_path_value,
            records_provider,
            subscription=None,
            publisher=None,
            callbacks=None
        )

        mocker.patch.object(
            records_provider, 'path_beginning_records',
            return_value=RecordsCppImpl(
                [
                    RecordCppImpl({
                        'callback_start/0': 0, 'pub/0': 1
                    }),
                ],
                [
                    ColumnValue('callback_start/0'),
                    ColumnValue('pub/0')
                ]
            )
        )
        records = node_path.to_path_beginning_records()
        expected = RecordsCppImpl(
            [
                RecordCppImpl({
                    'callback_start/0': 0, 'pub/0': 1
                }),
            ],
            [
                ColumnValue('callback_start/0'),
                ColumnValue('pub/0')
            ]
        )

        assert records.equals(expected)

    def test_end_path(self, mocker):
        records_provider = mocker.Mock(spec=RecordsProvider)
        node_path_value = mocker.Mock(spec=NodePathStructValue)
        node_path = NodePath(
            node_path_value,
            records_provider,
            subscription=None,
            publisher=None,
            callbacks=None
        )

        mocker.patch.object(
            records_provider, 'path_end_records',
            return_value=RecordsCppImpl(
                [
                    RecordCppImpl({
                        'callback_start/0': 0, 'callback_end/0': 1
                    }),
                ],
                [
                    ColumnValue('callback_start/0'),
                    ColumnValue('callback_end/0')
                ]
            )
        )
        records = node_path.to_path_end_records()

        expected = RecordsCppImpl(
            [
                RecordCppImpl({
                    'callback_start/0': 0, 'callback_end/0': 1
                }),
            ],
            [
                ColumnValue('callback_start/0'),
                ColumnValue('callback_end/0')
            ]
        )
        assert records.equals(expected)

    def test_to_records(self, mocker):
        records_provider = mocker.Mock(spec=RecordsProvider)
        node_path_value = mocker.Mock(spec=NodePathStructValue)
        node_path = NodePath(
            node_path_value,
            records_provider,
            subscription=None,
            publisher=None,
            callbacks=None
        )

        mocker.patch.object(
            records_provider, 'node_records',
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
        records = node_path.to_records()

        expected = RecordsCppImpl(
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

        assert records.equals(expected)

    def test_cache(self, mocker):
        records_provider = mocker.Mock(spec=RecordsProvider)
        node_path_value = mocker.Mock(spec=NodePathStructValue)
        node_path = NodePath(
            node_path_value,
            records_provider,
            subscription=None,
            publisher=None,
            callbacks=None
        )

        records_mock = mocker.Mock(spec=RecordsCppImpl)
        mocker.patch.object(node_path,
                            '_to_records_core', return_value=records_mock)
        mocker.patch.object(node_path,
                            '_to_path_beginning_records_core', return_value=records_mock)
        mocker.patch.object(node_path,
                            '_to_path_end_records_core', return_value=records_mock)

        node_path.to_records()
        assert node_path._to_records_core.call_count == 1  # type: ignore

        node_path.to_records()
        assert node_path._to_records_core.call_count == 1  # type: ignore

        node_path.clear_cache()
        node_path.to_records()
        assert node_path._to_records_core.call_count == 2  # type: ignore

        node_path.to_path_beginning_records()
        assert node_path._to_path_beginning_records_core.call_count == 1  # type: ignore

        node_path.to_path_beginning_records()
        assert \
            node_path._to_path_beginning_records_core.call_count == 1  # type: ignore

        node_path.clear_cache()
        node_path.to_path_beginning_records()
        assert \
            node_path._to_path_beginning_records_core.call_count == 2  # type: ignore

        node_path.to_path_end_records()
        assert node_path._to_path_end_records_core.call_count == 1  # type: ignore

        node_path.to_path_end_records()
        assert node_path._to_path_end_records_core.call_count == 1  # type: ignore

        node_path.clear_cache()
        node_path.to_path_end_records()
        assert node_path._to_path_end_records_core.call_count == 2  # type: ignore
