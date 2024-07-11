from caret_analyze import Architecture, Application, check_procedure, Lttng, LttngEventFilter
from caret_analyze.infra.lttng.lttng_event_filter import EventStripFilter, EventDurationFilter

def test_load():
    tracing_log_path = '/home/yamasaki/caret_script/analyze/szh_take_impl/e2e_sample'
    arch = Architecture('yaml', '/home/yamasaki/caret_script/analyze/szh_take_impl/yms_var_pass_architecture.yaml')
    lttng = Lttng(tracing_log_path)
    app = Application(arch, lttng)

    for target_path in arch.paths:
        print(target_path.path_name, target_path.verify())