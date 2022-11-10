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


from typing import Dict, Optional, Sequence

from .column import ColumnValue
from .record import Record, RecordInterface, Records, RecordsInterface

try:
    import caret_analyze.record.record_cpp_impl as cpp_impl

    use_cpp_impl = True
    print('Succeed to find record_cpp_impl. the C++ version will be used.')
except ModuleNotFoundError:
    use_cpp_impl = False
    print('Failed to find record_cpp_impl. the Python version will be used.')


class RecordFactory:
    """Factory class to create an instance of Record."""

    @classmethod
    def is_cpp_impl_valid(cls) -> bool:
        """
        Check whether cpp implementation is valid.

        Returns
        -------
        bool
            True if cpp implementation is valid, false otherwise.

        """
        return use_cpp_impl

    @classmethod
    def create_instance(cls, init: Optional[Dict] = None) -> RecordInterface:
        """
        Create record instance.

        Cpp implementations are used whenever possible.

        Parameters
        ----------
        init : Optional[Dict], optional
            Dictionary type data to be assigned as the content of record, by default None

        Returns
        -------
        RecordInterface
            Record instance.

        """
        if use_cpp_impl:
            return cls._create_cpp_instance(init)
        else:
            return Record(init)

    @classmethod
    def _create_cpp_instance(cls, init: Optional[Dict] = None) -> RecordInterface:
        # This function seems redundant.
        if init is None:
            return cpp_impl.RecordCppImpl()
        else:
            return cpp_impl.RecordCppImpl(init)


class RecordsFactory:
    """Factory class to create an instance of Records."""

    @staticmethod
    def is_cpp_impl_valid() -> bool:
        """
        Check whether cpp implementation is valid.

        Returns
        -------
        bool
            True if cpp implementation is valid, false otherwise.

        """
        return use_cpp_impl

    @staticmethod
    def create_instance(
        init: Optional[Sequence[RecordInterface]] = None,
        columns: Optional[Sequence[ColumnValue]] = None
    ) -> RecordsInterface:
        """
        Create records instance.

        Parameters
        ----------
        init : Optional[Sequence[RecordInterface]], optional
            Initial records data, by default None
        columns : Optional[Sequence[ColumnValue]], optional
            Column info of the records, by default None

        Returns
        -------
        RecordsInterface
            Records instance.

        """
        if use_cpp_impl:
            return RecordsFactory._create_cpp_instance(init, columns)
        else:
            return Records(init, columns)

    @staticmethod
    def _create_cpp_instance(
        init: Optional[Sequence[RecordInterface]] = None,
        columns: Optional[Sequence[ColumnValue]] = None,
    ) -> RecordsInterface:
        # This function seems redundant.
        return cpp_impl.RecordsCppImpl(init or [], columns or [])
