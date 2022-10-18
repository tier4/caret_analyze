from bokeh.plotting import figure, show

from caret_analyze.exceptions import UnsupportedTypeError

from caret_analyze.record import ResponseTime

from .histogram_interface import HistPlot


class ResponseTimePlot(HistPlot):

    def __init__(
        self,
        target,
        case='default'
    ):
        if case not in ['default', 'best', 'worst']:
            raise UnsupportedTypeError(
                f'Unsupported "case". case = {case}.'
                'supported "case": [default/best/worst]'
            )
        super().__init__(target)
        self._case = case

    def _show_core(self):
        for i, path in enumerate(self._target, 1):
            try:
                records = path.to_records()
                response = ResponseTime(records)
            except IndexError:
                pass

            if self._case == 'default':
                hist, bins = response.to_histogram(binsize_ns=10000000)
            elif self._case == 'best':
                hist, bins = response.to_best_case_histogram(binsize_ns=10000000)
            elif self._case == 'worst':
                hist, bins = response.to_worst_case_histogram(binsize_ns=10000000)

            p = figure(plot_width=600,
                       plot_height=400,
                       title=f'{path.path_name}',
                       active_scroll='wheel_zoom',
                       x_axis_label='Response Time [ms] (period: 10ms)',
                       y_axis_label='Probability')
            hist = hist / sum(hist)

            bins = bins*10**-6
            p.quad(top=hist, bottom=0, left=bins[:-1], right=bins[1:],
                   line_color='white', alpha=0.5)
            show(p)
