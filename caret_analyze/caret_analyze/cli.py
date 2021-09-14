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

from typing import List, Tuple

import fire

from . import Application, Lttng
from .callback import CallbackBase
from .plot import callback_grpah, chain_latency, message_flow
from .util import Util


class Create:

    def callback_graph(
        self, architecture_path: str, export_path: str, *callback_names: Tuple[str]
    ):
        app = Application(architecture_path, 'yaml', None)

        callbacks: List[CallbackBase] = []
        for name in callback_names:
            callback = Util.find_one(
                app.callbacks, lambda x: x.unique_name == name)
            if callback is None:
                print(f'Failed to find callback: {name}')
                return
            callbacks.append(callback)

        callback_grpah(app._arch, callbacks, export_path)

    def architecture(self, trace_dir: str, export_path: str):
        lttng = Lttng(trace_dir, force_conversion=True)
        app = Application(trace_dir, 'lttng', lttng)
        app.export_architecture(export_path)

    def chain_latency(
        self,
        trace_dir: str,
        architecture_path: str,
        path_name: str,
        export_path: str,
        granularity='node'
    ):
        lttng = Lttng(trace_dir, force_conversion=True)
        app = Application(architecture_path, 'yaml', lttng)
        path = app.path[path_name]
        chain_latency(path, export_path=export_path,
                      granularity=granularity)

    def message_flow(
        self,
        trace_dir: str,
        architecture_path: str,
        path_name: str,
        export_path: str,
        granularity='node'
    ):
        lttng = Lttng(trace_dir, force_conversion=True)
        app = Application(architecture_path, 'yaml', lttng)
        path = app.path[path_name]
        message_flow(path, export_path=export_path,
                     granularity=granularity)


def caret_create():
    fire.Fire(Create)
