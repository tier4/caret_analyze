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

from abc import abstractmethod
from typing import Callable, Dict, Iterator, List, Optional, Sequence, Set, Tuple

from multimethod import multimethod as singledispatchmethod
import pandas as pd

from .column import ColumnValue
from ..exceptions import InvalidArgumentError


class RecordInterface:
    """
    Interface for Record class.

    This behavior is similar to the dictionary type.
    To avoid conflicts with the pybind metaclass, ABC is not used.
    """

    @abstractmethod
    def equals(self, other: RecordInterface) -> bool:
        """
        Compare record.

        Parameters
        ----------
        other : RecordInterface
            comparison target.

        Returns
        -------
        bool
            True if record data is same, otherwise false.

        """
        pass

    @abstractmethod
    def merge(self, other: RecordInterface) -> None:
        """
        Merge record.

        Parameters
        ----------
        other : RecordInterface
            merge target.
        inplace : bool
            inplace record if true, otherwise false.

        Returns
        -------
        Record
            Merged record class if inplace = false, otherwise None.

        """
        pass

    @abstractmethod
    def drop_columns(self, columns: List[str]) -> None:
        """
        Drop columns method.

        Parameters
        ----------
        columns : List[str]
            columns to be dropped.

        """
        pass

    @abstractmethod
    def add(self, key: str, stamp: int) -> None:
        """
        Add(Update) column value.

        Parameters
        ----------
        key : str
            key name to set.
        stamp : int
            key value to set.

        """
        pass

    @abstractmethod
    def change_dict_key(self, old_key: str, new_key: str) -> None:
        """
        Change columns name.

        Parameters
        ----------
        old_key : str
            column name to be changed.
        new_key : str
            new column name.

        """
        pass

    @abstractmethod
    def get(self, key: str) -> int:
        """
        Get value for specific column.

        Parameters
        ----------
        key : str
            key name to get.

        Returns
        -------
        int
            Value for selected key.

        """
        pass

    @abstractmethod
    def get_with_default(self, key: str, v: int) -> int:
        """
        Get value for specific column.

        Parameters
        ----------
        key : str
            key name to get.
        v : int
            default value.

        Returns
        -------
        int
            Value for selected key.

        """
        pass

    @property
    @abstractmethod
    def data(self) -> Dict[str, int]:
        """
        Convert to dictionary.

        Returns
        -------
        data : Dict[str, int]:
            dictionary data.

        """
        pass

    @property
    @abstractmethod
    def columns(self) -> Set[str]:
        """
        Get column names.

        Returns
        -------
        Set[str]
            Column names.

        """
        pass


try:
    from record_cpp_impl import RecordBase
    # RecordBase in C++ implementation cannot inherit RecordInterface,
    # so RecordBase case should be written separately.
except ModuleNotFoundError as e:
    import os
    if 'GITHUB_ACTION' in os.environ:
        RecordBase = RecordInterface
    else:
        raise e


class RecordsInterface:
    """
    Interface for Record class.

    To avoid conflicts with the pybind metaclass, ABC is not used.
    """

    @abstractmethod
    def equals(self, other: RecordsInterface) -> bool:
        """
        Equals method.

        Parameters
        ----------
        other : RecordsInterface
            comparison target.

        Returns
        -------
        bool
            true if record data is same, otherwise false.

        """
        pass

    @singledispatchmethod
    def append(self, arg):
        raise InvalidArgumentError(f'Unknown argument type: {arg}')

    @append.register
    def __append_record(self, other: RecordInterface) -> None:
        self._append_record(other)

    @append.register
    def __append_record_base(self, other: RecordBase) -> None:
        self._append_record(other)  # type: ignore

    @abstractmethod
    def _append_record(self, other: RecordInterface) -> None:
        """
        Append new record.

        Parameters
        ----------
        other : RecordInterface
            record to be added.

        """
        pass

    @append.register
    def __append_dict(self, other: Dict[str, int]) -> None:
        self._append_dict(other)

    @abstractmethod
    def _append_dict(self, other: Dict[str, int]) -> None:
        """
        Append new record.

        Parameters
        ----------
        other : RecordInterface
            record to be added.

        """
        pass

    @abstractmethod
    def concat(self, other: RecordsInterface) -> None:
        """
        Concat records.

        Parameters
        ----------
        other : RecordsInterface
            records to be concatenated.

        Returns
        -------
        RecordsInterface
            concatenated records if inplace=False, otherwise None.

        """
        pass

    @abstractmethod
    def sort(
        self, key: str, sub_key: Optional[str] = None, ascending=True
    ) -> None:
        """
        Sort records.

        Parameters
        ----------
        key : str
            key name to used for sort.
        sub_key : str
            second key name to used for sort.
        ascending : bool
            ascending if True, descending if false.

        """
        pass

    @abstractmethod
    def sort_column_order(
        self,
        ascending: bool = True,
        put_none_at_top=True,
    ) -> None:
        """
        Sort records by ordered columns.

        Parameters
        ----------
        ascending : bool
            ascending if True, descending if false.
        put_none_at_top : bool

        """
        pass

    @abstractmethod
    def filter_if(
        self, f: Callable[[RecordInterface], bool]
    ) -> None:
        """
        Get filtered records.

        Parameters
        ----------
        f : Callable[[RecordInterface], bool]
            condition function.
        inplace : bool
            inplace original instance if true.

        """
        pass

    @property
    @abstractmethod
    def data(self) -> Sequence[RecordInterface]:
        """
        Get records list.

        Returns
        -------
        Sequence[RecordInterface]
            Records list.

        """
        pass

    @abstractmethod
    def get_row_series(self, index: int) -> RecordInterface:
        pass

    @abstractmethod
    def get_column_series(self, column_name: str) -> Sequence[Optional[int]]:
        pass

    def __len__(self) -> int:
        return len(self.data)

    def __iter__(self) -> Iterator:
        return iter(self.data)

    def reindex(self, columns: List[str]) -> None:
        """
        Reindex columns.

        Parameters
        ----------
        columns : List[str]

        """
        pass

    @abstractmethod
    def drop_columns(
        self, columns: List[str]
    ) -> None:
        """
        Drop columns.

        Parameters
        ----------
        columns : List[str]
            columns to be dropped.

        """
        pass

    @abstractmethod
    def rename_columns(
        self, columns: Dict[str, str]
    ) -> None:
        """
        Rename columns.

        Parameters
        ----------
        columns : Dict[str, str]
            rename params. same as dataframe rename.

        """
        pass

    @property
    @abstractmethod
    def columns(self) -> List[str]:
        """
        Get column names.

        Returns
        -------
        List[str]
            Columns.

        """
        pass

    @abstractmethod
    def to_dataframe(self) -> pd.DataFrame:
        """
        Convert to pandas dataframe.

        Returns
        -------
        pandas.DataFrame
            Records data.

        """
        pass

    @abstractmethod
    def merge(
        self,
        right_records: RecordsInterface,
        join_left_key: str,
        join_right_key: str,
        columns: List[str],
        how: str,
        *,
        progress_label: Optional[str] = None
    ) -> RecordsInterface:
        """
        Merge records by key match.

        Parameters
        ----------
        right_records : RecordInterface
            merge target.
        join_left_key : str
            Key to use for matching.
        join_right_key : str
            Key to use for matching.
        how : str
            merge type. [inner/right/left/outer]
        progress_label : str
            label for progress bar. cpp impl only.

        Returns
        -------
        RecordsInterface

        Example
        -------
        >>> left_records = Records([
            Record({'join_key': 1, 'left_other': 1}),
            Record({'join_key': 2, 'left_other': 2}),
        ])
        >>> right_records = Records([
            Record({'join_key': 2, 'right_other': 3}),
            Record({'join_key': 1, 'right_other': 4}),
        ])
        >>> expected = Records([
            Record({'join_key': 1, 'left_other': 1, 'right_other': 4}),
            Record({'join_key': 2, 'left_other': 2, 'right_other': 3}),
        ])
        >>> left_records.merge(right_records, 'join_key').equals(expected)
        True

        """
        pass

    @abstractmethod
    def merge_sequential(
        self,
        right_records: RecordsInterface,
        left_stamp_key: str,
        right_stamp_key: str,
        join_left_key: Optional[str],
        join_right_key: Optional[str],
        columns: List[str],
        how: str,
        *,
        progress_label: Optional[str] = None
    ) -> RecordsInterface:
        """
        Merge chronologically contiguous records.

        Merge left_records[left_key] and the right_records[right_key]
        that occurred immediately after it.
        If join_key is set, left_records[join_key]==right_records[join_key] is added as condition.


        Parameters
        ----------
        right_records : RecordsInterface
            merge target.
        left_stamp_key : str
            left records key name to use for comparison in time series merge.
        right_stamp_key : str
            right records key name to use for comparison in time series merge.
        join_key : str
            join key name to use equal condition.
        how : str
            merge type. [inner/right/left/outer]
        progress_label : str
            label for progress bar. cpp impl only.

        Records
        -------
        RecordsInterface
            Merged records.

        Example
        -------
        >>> left_records = Records([
            Record({'join_key': 1, 'left_stamp_key': 0}),
            Record({'join_key': 2, 'left_stamp_key': 3})
        ])
        >>> right_records = Records([
            Record({'join_key': 2, 'right_stamp_key': 5}),
            Record({'join_key': 1, 'right_stamp_key': 6})
        ])
        >>> expected = Records([
            Record({'join_key': 1, 'left_stamp_key': 0, 'right_stamp_key': 6}),
            Record({'join_key': 2, 'left_stamp_key': 3, 'right_stamp_key': 5}),
        ])
        >>> left_records.merge_sequential(
            right_records, 'left_stamp_key', 'right_stamp_key', 'join_key', 'inner'
        ).equals(expected)
        True


        """
        pass

    @abstractmethod
    def merge_sequential_for_addr_track(
        self,
        source_stamp_key: str,
        source_key: str,
        copy_records: RecordsInterface,
        copy_stamp_key: str,
        copy_from_key: str,
        copy_to_key: str,
        sink_records: RecordsInterface,
        sink_stamp_key: str,
        sink_from_key: str,
        columns: List[str],
        *,
        progress_label: Optional[str] = None
    ) -> RecordsInterface:
        """
        Merge for tracking addresses when copying occurs.

        Parameters
        ----------
        source_stamp_key : str
            key name indicating time stamp for source records
        source_key : str
            Key name indicating the address of the copy source for source records.
        copy_records : Recordsinterface
            copy records
        copy_stamp_key : str
            key name indicating time stamp for copy records
        copy_from_key : str
            Key name indicating the address of the copy source for source records.
        copy_to_key : str
            Key name indicating the address of the copy destination
        sink_records : RecordsInterface
            sink-side records
        sink_stamp_key : str
            keyname indicating time stamp for copy records
        sink_from_key : str
            Key name indicating the address of the copy destination
        progress_label : str
            label for progress bar. cpp impl only.

        Returns
        -------
        RecordsInterface
            Merged records.

        Examples
        --------
        >>> source_records = Records([
            Record({'source_key': 1, 'source_stamp': 0}),
        ])
        >>> copy_records = Records([
            Record({'copy_from_key': 1, 'copy_to_key': 11, 'copy_stamp_key': 1})
        ])
        >>> sink_records = Records([
            Record({'sink_from_key': 11, 'sink_stamp': 2}),
            Record({'sink_from_key': 1, 'sink_stamp': 3}),
        ])
        >>> expected = Records([
            Record({'source_stamp':0, 'sink_stamp':3, 'source_key':1}),
            Record({'source_stamp':0, 'sink_stamp':2, 'source_key':1}),
        ])
        >>> source_records.merge_sequential_for_addr_track(
            'source_stamp', 'source_key', copy_records, 'copy_stamp_key', 'copy_from_key',
            'copy_to_key', sink_records, 'sink_stamp', 'sink_from_key'
        ).equals(expected)
        True

        """
        pass

    @abstractmethod
    def append_column(self, column: ColumnValue, values: List[int]) -> None:
        """
        Append column to records.

        Parameters
        ----------
        column : ColumnValue
        values: List[int]

        """

    @abstractmethod
    def clone(self) -> RecordsInterface:
        """
        Get duplicated records.

        Returns
        -------
        RecordsInterface
            deep-copied records.

        """
        pass

    @abstractmethod
    def bind_drop_as_delay(self) -> None:
        """Convert the dropped points to records converted as delay."""
        pass

    @abstractmethod
    def groupby(self, columns: List[str]) -> Dict[Tuple[int, ...], RecordsInterface]:
        """Split based on the value of the given column name."""
        pass
