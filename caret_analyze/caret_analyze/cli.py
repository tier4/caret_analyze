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


import fire

from caret_analyze.plot import callback_grpah
from caret_analyze import Application


class Create:
    def callback_graph(self, architecture_path: str, callback_graph_path: str):
        app = Application(architecture_path, "yaml", None)
        callback_grpah(app._arch, callback_graph_path)


def caret_create():
    fire.Fire(Create)
