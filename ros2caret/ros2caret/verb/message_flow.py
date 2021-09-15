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
from caret_analyze.plot import message_flow
from ros2caret.verb import VerbExtension


class MessageFlowVerb(VerbExtension):

    def add_arguments(self, parser, cli_name):
        parser.add_argument(
            '-t', '--trace_dir', dest='trace_dir',
            help='trace dir', required=True)

        parser.add_argument(
            '-a', '--architecture_path', dest='architecture_path',
            help='architecture', required=True)

        parser.add_argument(
            '-o', '--output_path', dest='output_path',
            help='output path to the message flow file', required=True)

        parser.add_argument(
            '-p', '--path_name', dest='path_name',
            help='path name of trace points to be visualized', required=True)

        parser.add_argument(
            '-g', '--granularity', dest='granularity', default=None,
            help='granularity of trace points to be visualized')

    def main(self, *, args):
        lttng = Lttng(args.trace_dir, force_conversion=True)
        app = Application(args.architecture_path, 'yaml', lttng)
        path = app.path[args.path_name]

        message_flow(path, export_path=args.output_path,
                     granularity=args.granularity)
