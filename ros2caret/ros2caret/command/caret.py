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

from ros2cli.command import add_subparsers_on_demand, CommandExtension


class CaretCommand(CommandExtension):

    def add_arguments(self, parser, cli_name):
        self._subparser = parser
        add_subparsers_on_demand(
            parser, cli_name, '_verb', 'ros2caret.verb', required=False
        )

    def main(self, *, parser, args):
        if not hasattr(args, '_verb'):
            self._subparser.print_help()
            return 0
        extension = getattr(args, '_verb')

        return extension.main(args=args)
