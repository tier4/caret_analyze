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
# limitations under the License.from caret_analyze import Application, Lttng

from caret_analyze import Application, Lttng
from ros2caret.verb import VerbExtension


class ArchitectureVerb(VerbExtension):

    def add_arguments(self, parser, cli_name):
        parser.add_argument(
            '-t', '--trace_directory', dest='trace_directory', type=str,
            help='the path to the main trace directory results path', required=True)

        parser.add_argument(
            '-o', '--output_path', dest='output_path', type=str,
            help='the path to the architecture file', required=True)

    def main(self, *, args):
        lttng = Lttng(args.trace_directory, force_conversion=True)
        app = Application(args.trace_directory, 'lttng', lttng)
        app.export_architecture(args.output_path)
