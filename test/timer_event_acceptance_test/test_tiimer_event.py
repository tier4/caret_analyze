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

from caret_analyze.architecture.architecture import Architecture
from caret_analyze.infra.lttng import Lttng, RecordsProviderLttng
from caret_analyze.infra.lttng.lttng import LttngEventFilter
from caret_analyze.runtime.application import Application
from caret_analyze.value_objects import CallbackGroupType, TimerStructValue
# from caret_analyze.plot import callback_sched

import pandas as pd

from caret_analyze.value_objects.callback import TimerCallbackStructValue


class TestEndToEndDemo:

    def test_timer_events_factory(self):
        lttng = Lttng('./test/timer_event_acceptance_test/end_to_end_sample',
                      force_conversion=True,
                      use_singleton_cache=False)
        nodes = lttng.get_nodes()
        timer_node = next(filter(lambda x: x.node_name == '/timer_driven_node', nodes))
        timer_cb = lttng.get_timer_callbacks(timer_node)[0]

        timer_events_factory = lttng.create_timer_events_factory(timer_cb)
        last_callback_start_ns = 1641466702532256085
        until_ns = last_callback_start_ns + 1e10
        timer_events = timer_events_factory.create(until_ns)
        df = timer_events.to_dataframe()
        # TODO: check df
        # timer eventsのカラムがuntil_nsまで続くことを確認
        []
        pass

    def test_timer_events(self):
        lttng = Lttng('./develop/end_to_end_sample',
                      force_conversion=True,
                      use_singleton_cache=False)

        nodes = lttng.get_nodes()
        timer_node = next(filter(lambda x: x.node_name == '/timer_driven_node', nodes))
        timer_cb = lttng.get_timer_callbacks(timer_node)[0]

        provider = RecordsProviderLttng(lttng)

        timer_cb_struct = TimerCallbackStructValue(
            timer_node.node_name, timer_cb.symbol, timer_cb.period_ns, None, 'callback_name')
        timer_cb = TimerStructValue(timer_node.node_name, timer_cb.period_ns, timer_cb_struct)

        timer_records = provider.timer_records(timer_cb)
        df = timer_records.to_dataframe()
        # timer_events, callback_start, callback_endのカラムを確認。

    def test_fimer_fire(self):
        arch = Architecture('yaml', './test/timer_event_acceptance_test/arch_e2e_sample.yaml')
        lttng = Lttng('./develop/end_to_end_sample',
                      force_conversion=True,
                      use_singleton_cache=False)
        app = Application(arch, lttng)

        node = app.get_node('/timer_driven_node')
        names = node.callback_names
        timer_cb = node.get_callback('/timer_driven_node/callback_0')
        assert timer_cb.subscription is None
        assert timer_cb.timer is not None

        timer = timer_cb.timer
        df = timer.to_dataframe()
        # TODO: check df

        sub_cb = node.get_callback('/timer_driven_node/callback_1')
        sub_cb.publishers[0].to_dataframe

        assert sub_cb.subscription is not None
        assert sub_cb.timer is None
