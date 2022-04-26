
from __future__ import annotations

from typing import (
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
)

from .callback import CallbackStruct
from .struct_interface import VariablePassingsStructInterface, VariablePassingStructInterface
from ...value_objects import VariablePassingStructValue


class VariablePassingStruct(VariablePassingStructInterface):

    def __init__(
        self,
        node_name: Optional[str] = None,
        callback_write: Optional[CallbackStruct] = None,
        callback_read: Optional[CallbackStruct] = None,
    ) -> None:
        self._node_name = node_name
        self._callback_write = callback_write
        self._callback_read = callback_read

    @property
    def node_name(self) -> str:
        assert self._node_name is not None
        return self._node_name

    @property
    def callback_write(self) -> CallbackStruct:
        assert self._callback_write is not None
        return self._callback_write

    @property
    def callback_read(self) -> CallbackStruct:
        assert self._callback_read is not None
        return self._callback_read

    @property
    def callback_name_read(self) -> Optional[str]:
        return self.callback_read.callback_name

    @property
    def callback_name_write(self) -> Optional[str]:
        return self.callback_write.callback_name

    def to_value(self) -> VariablePassingStructValue:
        return VariablePassingStructValue(
            node_name=self.node_name,
            callback_write=self.callback_write.to_value(),
            callback_read=self.callback_read.to_value(),
        )


class VariablePassingsStruct(VariablePassingsStructInterface, Iterable):

    def __init__(self) -> None:
        self._data: List[VariablePassingStruct] = []

    def add(self, var_pass: VariablePassingStruct):
        self._data.append(var_pass)

    def __iter__(self) -> Iterator[VariablePassingStruct]:
        return iter(self._data)

    def to_value(self) -> Tuple[VariablePassingStructValue, ...]:
        return tuple(_.to_value() for _ in self._data)
