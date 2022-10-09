from abc import ABCMeta, abstractmethod

from bokeh.plotting import ColumnDataSource, Figure, figure, save, show


class HistPlot(metaclass=ABCMeta):
	def __init__(
		self,
		target
	):
		super().__init__()

	def show(
		self
	):

		return self._show_core()

	@abstractmethod
	def _show_core(self):
		pass
