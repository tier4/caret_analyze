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


class Error(Exception):
    """Base class for exception in this module."""


class TraceResultAanalyzeError(Error):
    """Failed to parse trace results."""

    def __init__(self, *args: object) -> None:
        """Construct exception instance."""
        super().__init__(*args)


class InvalidYamlFormatError(Error):
    """Failed to load yaml."""

    def __init__(self, message: str) -> None:
        self.message = message


class InvalidTraceFormatError(Error):
    """Failed to load trace data."""

    def __init__(self, message: str) -> None:
        self.message = message


class InvalidReaderError(Error):
    """Failed to load architecutre."""

    def __init__(self, message: str) -> None:
        self.message = message


class ItemNotFoundError(Error):
    """Failed to identify item that match the condition."""

    def __init__(self, message: str) -> None:
        self.message = message


class MultipleItemFoundError(Error):
    """Failed to identify item that match the condition."""

    def __init__(self, message: str) -> None:
        self.message = message


class InvalidArgumentError(Error):
    """Failed to process function."""

    def __init__(self, message: str) -> None:
        self.message = message


class UnsupportedTypeError(Error):
    """Given type is unsupported."""

    def __init__(self, message: str) -> None:
        self.message = message


class InvalidRecordsError(Error):
    """Given Records does not have the necessary columns."""

    def __init__(self, message: str) -> None:
        self.message = message


class UnsupportedNodeRecordsError(Error):
    """Failed to calculate node path records."""

    def __init__(self, message: str) -> None:
        self.message = message
