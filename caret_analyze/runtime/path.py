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

from __future__ import annotations

from logging import getLogger
from typing import List, Optional, Union

from caret_analyze.record.column import ColumnAttribute

from .callback import CallbackBase
from .communication import Communication, TransformCommunication
from .node_path import NodePath
from .path_base import PathBase
from ..common import Summarizable, Summary, Util
from ..exceptions import InvalidArgumentError, InvalidRecordsError
from ..record.record import merge, merge_sequencial, RecordsInterface
from ..value_objects import PathStructValue

logger = getLogger(__name__)


class RecordsMerged:

    def __init__(
        self,
        merge_targets: List[Union[NodePath, Communication, TransformCommunication]],
    ) -> None:
        if len(merge_targets) == 0:
            raise InvalidArgumentError('There are no records to be merged.')
        self._data = self._merge_records(merge_targets)

    @property
    def data(self) -> RecordsInterface:
        return self._data

    @staticmethod
    def _merge_records(
        targets: List[Union[NodePath, Communication, TransformCommunication]]
    ) -> RecordsInterface:
        logger.info('Started merging path records.')

        if len(targets[0].to_records().data) == 0:
            targets = targets[1:]

        first_element = targets[0].to_records([
            ColumnAttribute.SYSTEM_TIME,
            ColumnAttribute.NODE_IO])
        left_records = first_element

        for column in left_records.columns:
            column.add_prefix('0')

        first_column = first_element.columns[0]

        for i, (target_, target) in enumerate(zip(targets[:-1], targets[1:])):
            if isinstance(target, NodePath) and target.message_context is None:
                if target == targets[-1]:
                    msg = 'Detected dummy_records. merge terminated.'
                    logger.info(msg)
                else:
                    msg = 'Detected dummy_records before merging end_records. merge terminated.'
                    logger.warn(msg)
                break

            right_records: RecordsInterface = target.to_records([
                ColumnAttribute.SYSTEM_TIME,
                ColumnAttribute.NODE_IO
            ])

            right_records.columns[0].add_prefix(f'{i}')
            right_records.columns[1].add_prefix(f'{i+1}')
            # rename_rule = column_merger.append_columns_and_return_rename_rule(
            #     right_records)
            # right_records.columns.rename(rename_rule)

            # right_column = right_records.column_names[0]
            # match_column_index = [i for (i, column)
            #                       in enumerate(left_records.column_names)
            #                       if column == right_column][0]
            # drop_left_columns = left_records.column_names[match_column_index+1:]
            # left_records.columns.drop(drop_left_columns)

            if left_records.column_names[-1] != right_records.column_names[0]:
                raise InvalidRecordsError('left columns[-1] != right columns[0]')
            left_stamp_key = left_records.columns[-1]
            right_stamp_key = right_records.columns[0]

            # right_records.columns.drop([left_records.column_names[0]])
            right_stamp_key = right_records.columns[0]

            logger.info(
                '\n[merge_sequencial] \n'
                f'- left_column: {left_stamp_key} \n'
                f'- right_column: {right_stamp_key} \n'
            )

            # is_sequencial = isinstance(target_, NodePath) and \
            #     isinstance(target, Communication)
            is_sequencial = False

            if is_sequencial:
                left_records = merge_sequencial(
                    left_records=left_records,
                    right_records=right_records,
                    join_left_key=None,
                    join_right_key=None,
                    left_stamp_key=left_stamp_key.column_name,
                    right_stamp_key=right_stamp_key.column_name,
                    how='left_use_latest',
                    progress_label='binding: node records'
                )
            else:
                # tf to tf がpublish recordsとうまく結合されていない。
                left_records = merge(
                    left_records=left_records,
                    right_records=right_records,
                    join_left_key=left_records.column_names[-1],
                    join_right_key=right_records.column_names[0],
                    how='left'
                )

        logger.info('Finished merging path records.')
        left_records.sort(first_column.column_name)

        return left_records


class Path(PathBase, Summarizable):
    def __init__(
        self,
        path: PathStructValue,
        child: List[Union[NodePath, Communication, TransformCommunication]],
        callbacks: Optional[List[CallbackBase]]
    ) -> None:
        super().__init__()

        self._value = path
        self._validate(child)
        self._child = child
        self._columns_cache: Optional[List[str]] = None
        self._callbacks = callbacks
        return None

    def _to_records_core(self) -> RecordsInterface:
        self._verify_path(self.node_paths)
        return RecordsMerged(self.child).data

    @staticmethod
    def _verify_path(
        path_children: List[NodePath]
    ) -> None:
        for child in path_children[1: -1]:
            if len(child.column_names) == 0:
                msg = 'Node latency is empty. To see more details, execute [ path.verify() ].'
                logger.warning(msg)

    def verify(self) -> bool:
        is_valid = True
        for child in self.node_paths[1:-1]:
            if child.message_context is not None:
                continue
            msg = 'Detected invalid message context. Correct these node_path definitions. \n'
            msg += 'To see node definition,'
            msg += f"execute [ app.get_node('{child.node_name}').summary.pprint() ] \n"
            msg += str(child.summary)
            logger.warning(msg)
            is_valid = False

        for comm in self.communications:
            is_valid &= comm.verify()
        return is_valid

    def get_child(self, name: str):
        if not isinstance(name, str):
            raise InvalidArgumentError('Argument type is invalid.')

        def is_target(child: Union[NodePath, Communication]):
            if isinstance(child, NodePath):
                return child.node_name == name
            elif isinstance(child, Communication):
                return child.topic_name == name

        return Util.find_one(is_target, self.child)

    @property
    def summary(self) -> Summary:
        return self._value.summary

    @property
    def callbacks(self) -> Optional[List[CallbackBase]]:
        return self._callbacks

    @staticmethod
    def _validate(
        path_elements: List[Union[NodePath, Communication, TransformCommunication]]
    ) -> None:
        if len(path_elements) == 0:
            return

        is_node_path = [isinstance(e, NodePath) for e in path_elements]
        is_node_path_indexes = [i for i, _ in enumerate(is_node_path) if _ is True]
        is_even_number = [i % 2 == 0 for i in is_node_path_indexes]

        if len(set(is_even_number)) >= 2:
            msg = 'NodePath and Communication should be alternated.'
            raise InvalidArgumentError(msg)

    @property
    def path_name(self) -> Optional[str]:
        return self._value.path_name

    def clear_cache(self) -> None:
        self._columns_cache = None
        return super().clear_cache()

    def __str__(self) -> str:
        node_names = [n.node_name for n in self.node_paths]
        return '\n'.join(node_names)

    @property
    def communications(self) -> List[Communication]:
        return Util.filter_items(lambda x: isinstance(x, Communication), self._child)

    @property
    def node_paths(self) -> List[NodePath]:
        return Util.filter_items(lambda x: isinstance(x, NodePath), self._child)

    @property
    def topic_names(self) -> List[str]:
        return sorted(self._value.topic_names)

    @property
    def child(self) -> List[Union[NodePath, Communication, TransformCommunication]]:
        return self._child

    @property
    def child_names(self) -> List[str]:
        return sorted(self._value.child_names)

    @property
    def node_names(self) -> List[str]:
        return sorted(self._value.node_names)
