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

from __future__ import annotations

from collections.abc import Sequence
from logging import getLogger

from .callback import CallbackBase
from .communication import Communication
from .node_path import NodePath
from .path_base import PathBase
from ..common import Summarizable, Summary, Util
from ..exceptions import Error, InvalidArgumentError, InvalidRecordsError
from ..record import (Columns,
                      merge, merge_sequential,
                      RecordsFactory,
                      RecordsInterface)
from ..value_objects import CallbackChain, PathStructValue

logger = getLogger(__name__)


class ColumnMerger():

    def __init__(self) -> None:
        self._count: dict[str, int] = {}
        self._column_names: list[str] = []

    def append_column(
        self,
        records: RecordsInterface,
        column_name: str
    ) -> str:
        """
        Append column.

        Parameters
        ----------
        records : RecordsInterface
            Records interface.
        column_name : str
            Column name.

        Returns
        -------
        str
            Append column name.

        """
        key = column_name

        if key == records.columns[0] and len(self._count) > 0 and key in self._count.keys():
            count = max(self._count.get(key, 0) - 1, 0)
            return self._to_column_name(count, key)

        if key not in self._count.keys():
            self._count[key] = 0

        column_name = self._to_column_name(self._count[key], key)
        self._column_names.append(column_name)
        self._count[key] += 1
        return column_name

    def append_columns(
        self,
        records: RecordsInterface,
    ) -> list[str]:
        """
        Append columns.

        Parameters
        ----------
        records : RecordsInterface
            Records interface.

        Returns
        -------
        list[str]
            Renamed columns.

        """
        renamed_columns: list[str] = []
        for column in records.columns:
            renamed_columns.append(
                self.append_column(records, column)
            )
        return renamed_columns

    def append_columns_and_return_rename_rule(
        self,
        records: RecordsInterface,
    ) -> dict[str, str]:
        """
        Append columns and return rename rule.

        Parameters
        ----------
        records : RecordsInterface
            Records interface.

        Returns
        -------
        dict[str, str]
            Renamed rule.

        """
        renamed_columns: list[str] = self.append_columns(records)
        return self._to_rename_rule(records.columns, renamed_columns)

    @property
    def column_names(
        self
    ) -> list[str]:
        """
        Get column names.

        Returns
        -------
        list[str]
            Column names.

        """
        return self._column_names

    @staticmethod
    def _to_column_name(
        count: int,
        tracepoint_name: str,
    ) -> str:
        return f'{tracepoint_name}/{count}'

    @staticmethod
    def _to_rename_rule(
        old_columns: Sequence[str],
        new_columns: Sequence[str],
    ):
        return dict(zip(old_columns, new_columns))


class RecordsMerged:

    def __init__(
        self,
        merge_targets: list[NodePath | Communication],
        include_first_callback: bool = False,
        include_last_callback: bool = False
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        merge_targets : list[NodePath | Communication]
            Merge targets.
        include_first_callback : bool
            Flags for including the processing time of the first callback in the path analysis.
        include_last_callback : bool
            Flags for including the processing time of the last callback in the path analysis.

        Raises
        ------
        InvalidArgumentError
            There are no records to be merged.

        """
        if len(merge_targets) == 0:
            raise InvalidArgumentError('There are no records to be merged.')
        self._data = self._merge_records(
            merge_targets,
            include_first_callback,
            include_last_callback)

    @property
    def data(self) -> RecordsInterface:
        """
        Get data.

        Returns
        -------
        RecordsInterface
            Records data.

        """
        return self._data

    @staticmethod
    def _merge_records(
        targets: list[NodePath | Communication],
        include_first_callback: bool = False,
        include_last_callback: bool = False
    ) -> RecordsInterface:
        logger.info('Started merging path records.')

        def is_match_column(column: str, target_name: str) -> bool:
            last_slash_index = column.rfind('/')
            if last_slash_index >= 0:
                column = column[:last_slash_index]
            return column.endswith(target_name)

        column_merger = ColumnMerger()
        if include_first_callback and isinstance(targets[0], NodePath):
            first_element = targets[0].to_path_beginning_records()
        else:
            if len(targets[0].to_records().data) == 0:
                # If the first element has no data, skip it and re-evaluate targets
                targets = targets[1:]
                if not targets:  # Check if targets list became empty after removal
                    msg = (
                        'Targets list became empty after filtering out first element. '
                    )
                    raise InvalidRecordsError(msg)
            first_element = targets[0].to_records()

        left_records: RecordsInterface = first_element

        rename_rule = column_merger.append_columns_and_return_rename_rule(left_records)
        left_records.rename_columns(rename_rule)
        # Handle case where first_element might have no columns
        first_column = left_records.columns[0] if left_records.columns else None

        last_communication = None
        for item in reversed(targets):
            if isinstance(item, Communication):
                last_communication = item
                break

        if not last_communication:
            raise InvalidRecordsError('Communication record does not exist')

        take_records_applied_for_last_communication: bool = False
        for target_, target in zip(targets[:-1], targets[1:]):
            right_records: RecordsInterface = target.to_records()

            is_dummy_records = len(right_records.columns) == 0

            if is_dummy_records:
                if target == targets[-1]:
                    msg = 'Detected dummy_records. merge terminated.'
                    logger.info(msg)
                else:
                    msg = 'Detected dummy_records before merging end_records. merge terminated.'
                    logger.warning(msg)
                break
            should_drop_last_column = False
            if isinstance(target, Communication) and target.use_take_manually():
                if target == last_communication:
                    try:
                        right_records = target.to_take_records()
                        take_records_applied_for_last_communication = True
                    except Exception as e:
                        msg = f'Failed to get take records for the last Communication record: {e}'
                        logger.error(msg)
                        raise InvalidRecordsError(msg)
                else:
                    should_drop_last_column = True

            if should_drop_last_column:
                if len(right_records.columns) > 0:
                    right_records.drop_columns([right_records.columns[-1]])

            rename_rule = column_merger.append_columns_and_return_rename_rule(right_records)
            right_records.rename_columns(rename_rule)

            if left_records.columns[-1] != right_records.columns[0]:
                raise InvalidRecordsError(
                    f'left: {left_records.columns[-1]} != right: {right_records.columns[0]}'
                )

            left_stamp_key = left_records.columns[-1]
            right_stamp_key = right_records.columns[0]

            logger.info(
                '\n[merge_sequential] \n'
                f'- left_column: {left_stamp_key} \n'
                f'- right_column: {right_stamp_key} \n'
            )

            is_sequential = (
                isinstance(target_, NodePath) and
                isinstance(target, Communication) and
                isinstance(target_.message_context, CallbackChain)
            )

            if is_sequential:
                left_records = merge_sequential(
                    left_records=left_records,
                    right_records=right_records,
                    join_left_key=None,
                    join_right_key=None,
                    left_stamp_key=left_stamp_key,
                    right_stamp_key=right_stamp_key,
                    columns=Columns.from_str(
                        left_records.columns + right_records.columns
                    ).column_names,
                    how='left_use_latest',
                )
            else:
                left_records = merge(
                    left_records=left_records,
                    right_records=right_records,
                    join_left_key=left_records.columns[-1],
                    join_right_key=right_records.columns[0],
                    columns=Columns.from_str(
                        left_records.columns + right_records.columns
                    ).column_names,
                    how='left'
                )

        if include_last_callback and isinstance(targets[-1], NodePath):
            output_log = False
            if take_records_applied_for_last_communication:
                output_log = True
            elif is_match_column(left_records.columns[-1], 'source_timestamp'):
                output_log = True

            if output_log:
                msg = 'Since the path cannot be extended, '
                msg += 'the merge process for the last callback record is skipped.'
                logger.warning(msg)
            else:
                right_records = targets[-1].to_path_end_records()
                rename_rule = column_merger.append_columns_and_return_rename_rule(right_records)
                right_records.rename_columns(rename_rule)

                if left_records.columns[-1] != right_records.columns[0]:
                    raise InvalidRecordsError(
                        f'left: {left_records.columns[-1]} != right: {right_records.columns[0]}'
                    )
                else:
                    if len(right_records.data) != 0:
                        left_records = merge(
                            left_records=left_records,
                            right_records=right_records,
                            join_left_key=left_records.columns[-1],
                            join_right_key=right_records.columns[0],
                            columns=Columns.from_str(
                                left_records.columns + right_records.columns
                            ).column_names,
                            how='left'
                        )
                    else:
                        msg = 'Empty records are not merged.'
                        logger.warning(msg)

        logger.info('Finished merging path records.')
        if first_column is not None:
            left_records.sort(first_column)
        else:
            raise InvalidRecordsError('first column not in columns')

        # remove source_timestamp columns
        source_columns = [
            column for column in left_records.columns
            if is_match_column(column, 'source_timestamp')
        ]
        left_records.drop_columns(source_columns)

        # remove rmw_take columns except for the last one
        rmw_cols = [col for col in left_records.columns if ('rmw_take' in col)]
        if take_records_applied_for_last_communication:
            drop_rmw_cols = rmw_cols[:-1]
        else:
            drop_rmw_cols = rmw_cols
        left_records.drop_columns(drop_rmw_cols)

        return left_records


class Path(PathBase, Summarizable):
    """
    A class that represents a path.

    A single path is composed of node paths and communications.
    """

    def __init__(
        self,
        path: PathStructValue,
        child: list[NodePath | Communication],
        callbacks: list[CallbackBase] | None,
        include_first_callback: bool = False,
        include_last_callback: bool = False
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        path : PathStructValue
            static info
        child : list[NodePath | Communication]
            path children's which compose path (node paths and communications).
        callbacks : list[CallbackBase] | None
            callbacks that compose the path.
            return None except for all of node paths are not callback-chain.
        include_first_callback : bool
            Flags for including the processing time of the first callback in the path analysis.
        include_last_callback : bool
            Flags for including the processing time of the last callback in the path analysis.

        """
        super().__init__()

        self._value = path
        self._validate(child)
        self._child = child
        self._columns_cache: list[str] | None = None
        self._callbacks = callbacks
        self._include_first_callback = include_first_callback
        self._include_last_callback = include_last_callback
        self.__records_cache: dict = {}
        return None

    @property
    def include_first_callback(self) -> bool:
        """
        Get include first callback.

        Returns
        -------
        bool
            Flags for including the processing time of the first callback in the path analysis.

        """
        return self._include_first_callback

    @include_first_callback.setter
    def include_first_callback(self, include_first_callback: bool) -> None:
        self._include_first_callback = include_first_callback

    @property
    def include_last_callback(self) -> bool:
        """
        Get include last callback.

        Returns
        -------
        bool
            Flags for including the processing time of the last callback in the path analysis.

        """
        return self._include_last_callback

    @include_last_callback.setter
    def include_last_callback(self, include_last_callback: bool) -> None:
        self._include_last_callback = include_last_callback

    def to_records(self) -> RecordsInterface:
        """
        Calculate records.

        Returns
        -------
        RecordsInterface
            Execution time of each operation.

        """
        if (self._include_first_callback, self._include_last_callback) \
                not in self.__records_cache.keys():
            try:
                self.__records_cache[(self._include_first_callback, self._include_last_callback)] \
                    = self._to_records_core()
            except Error as e:
                logger.warning(e)
                self.__records_cache[(self._include_first_callback, self._include_last_callback)] \
                    = RecordsFactory.create_instance()

        assert (self._include_first_callback, self._include_last_callback) \
            in self.__records_cache.keys()
        return self.__records_cache[(self._include_first_callback,
                                     self._include_last_callback)].clone()

    def _to_records_core(self) -> RecordsInterface:
        self._verify_path(self.node_paths)
        return RecordsMerged(self.child,
                             self._include_first_callback, self._include_last_callback).data

    @staticmethod
    def _verify_path(
        path_children: list[NodePath]
    ) -> None:
        for child in path_children[1: -1]:
            if len(child.column_names) == 0:
                msg = 'Node latency is empty. To see more details, execute [ path.verify() ].'
                logger.warning(msg)

    def verify(self) -> bool:
        """
        Verify whether the path can generate latencies.

        Returns
        -------
        bool
            True if both architecture and measurement results are valid, otherwise false.

        """
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
        """
        Get child.

        Parameters
        ----------
        name : str
            Topic name or node name.

        Raises
        ------
        InvalidArgumentError
            Occurs when the given argument type is invalid.
        ItemNotFoundError
            Occurs when no items were found.
        MultipleItemFoundError
            Occurs when several items were found.

        """
        # TODO(hsgwa): This function is not needed. Remove.

        if not isinstance(name, str):
            raise InvalidArgumentError('Argument type is invalid.')

        def is_target(child: NodePath | Communication):
            if isinstance(child, NodePath):
                return child.node_name == name
            elif isinstance(child, Communication):
                return child.topic_name == name

        return Util.find_one(is_target, self.child)

    @property
    def summary(self) -> Summary:
        """
        Get summary [override].

        Returns
        -------
        Summary
            summary info.

        """
        return self._value.summary

    @property
    def callbacks(self) -> list[CallbackBase]:
        """
        Get callbacks.

        Returns
        -------
        list[CallbackBase]
            callbacks in all nodes that comprise the node path.

        """
        callbacks = Util.flatten(
            comm.publish_node.callbacks for comm in self.communications
            if comm.publish_node.callbacks
        )
        if self.communications[-1].subscribe_node.callbacks is not None:
            callbacks.extend(self.communications[-1].subscribe_node.callbacks)

        return callbacks

    @property
    def callback_chain(self) -> list[CallbackBase] | None:
        """
        Get callback chain.

        Returns
        -------
        list[CallbackBase] | None
            callbacks that compose the path.
            return None except for all of the node paths are callback chains.

        """
        return self._callbacks

    @staticmethod
    def _validate(path_elements: list[NodePath | Communication]):
        if len(path_elements) == 0:
            return
        t = NodePath if isinstance(
            path_elements[0], NodePath) else Communication
        for e in path_elements[1:]:
            if t == Communication:
                t = NodePath
            else:
                t = Communication
            if isinstance(e, t):
                continue
            msg = 'NodePath and Communication should be alternated.'
            raise InvalidArgumentError(msg)

    @property
    def path_name(self) -> str | None:
        """
        Get path name.

        Returns
        -------
        str | None
            Path name defined in the architecture.

        """
        return self._value.path_name

    @property
    def value(self) -> PathStructValue:
        """
        Get StructValue object.

        Returns
        -------
        PathStructValue
            path value.

        Notes
        -----
        This property is for CARET debugging purposes.

        """
        return self._value

    def clear_cache(self) -> None:
        """Clear to_records/to_dataframe cache."""
        self._columns_cache = None
        self.__records_cache = {}
        return super().clear_cache()

    def __str__(self) -> str:
        node_names = [n.node_name for n in self.node_paths]
        return '\n'.join(node_names)

    @property
    def communications(self) -> list[Communication]:
        """
        Get communications.

        Returns
        -------
        list[Communication]
            Communications in target path.

        """
        return Util.filter_items(lambda x: isinstance(x, Communication), self._child)

    @property
    def node_paths(self) -> list[NodePath]:
        """
        Get node-paths.

        Returns
        -------
        list[NodePath]
            node paths in target path.

        """
        return Util.filter_items(lambda x: isinstance(x, NodePath), self._child)

    @property
    def topic_names(self) -> list[str]:
        """
        Get topic names.

        Returns
        -------
        list[str]
            topic names in the target path.

        """
        return sorted(self._value.topic_names)

    @property
    def child(self) -> list[NodePath | Communication]:
        """
        Get path children.

        Returns
        -------
        list[NodePath | Communication]
            node paths and communications in the target path.
            node paths and communications are alternately contained.

        """
        return self._child

    @property
    def child_names(self) -> list[str]:
        """
        Get path children's names.

        Returns
        -------
        list[str]
            node names and topic names in the target path.

        """
        return sorted(self._value.child_names)

    @property
    def node_names(self) -> list[str]:
        """
        Get node names.

        Returns
        -------
        list[str]
            node names in the target path.

        """
        return sorted(self._value.node_names)
