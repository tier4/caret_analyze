from caret_analyze import Architecture, Application, check_procedure, Lttng, LttngEventFilter
from caret_analyze.infra.lttng.lttng_event_filter import EventStripFilter, EventDurationFilter
from caret_analyze.plot import Plot

def test_load():
    tracing_log_path = '/home/yamasaki/caret_script/analyze/szh_take_impl/e2e_sample'
    # arch = Architecture('yaml', '/home/yamasaki/caret_script/analyze/szh_take_impl/yms_var_pass_architecture.yaml')
    arch = Architecture('yaml', '/home/yamasaki/caret_script/analyze/szh_take_impl/yms_architecture_path_fixed.yaml')
    lttng = Lttng(tracing_log_path)
    app = Application(arch, lttng)

    path_name = 'start_from_B_0'

    path = app.get_path(path_name)
    path.include_first_callback = True
    path.include_last_callback = True
    # Plot.create_message_flow_plot(path)
    path.to_dataframe()
