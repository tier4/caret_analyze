from abc import ABCMeta, abstractmethod
from typing import Collection

from ...runtime import Path


class HistPlot(metaclass=ABCMeta):
    def __init__(
        self,
        target: Collection[Path]
    ) -> None:
        self._target = list(target)

    def show(
        self
    ):
        return self._show_core()

    @abstractmethod
    def _show_core(self):
        pass
