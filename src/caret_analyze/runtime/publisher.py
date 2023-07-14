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

from __future__ import annotations

from .path_base import PathBase
from ..common import Summarizable, Summary
from ..infra import RecordsProvider, RuntimeDataProvider
from ..record import RecordsInterface
from ..value_objects import PublisherStructValue, Qos


class Publisher(PathBase, Summarizable):
    """Class that represents publisher."""

    def __init__(
        self,
        publisher: PublisherStructValue,
        provider: RecordsProvider | RuntimeDataProvider,
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        publisher : PublisherStructValue
            static info.
        provider : RecordsProvider | RuntimeDataProvider
            provider to be evaluated.

        """
        super().__init__()
        self._val = publisher
        self._provider = provider

    @property
    def node_name(self) -> str:
        """
        Get node name.

        Returns
        -------
        str
            node name which contains the publisher.

        """
        return self._val.node_name

    @property
    def summary(self) -> Summary:
        """
        Get summary [override].

        Returns
        -------
        Summary
            summary info.

        """
        return self._val.summary

    @property
    def topic_name(self) -> str:
        """
        Get a topic name.

        Returns
        -------
        str
            A topic name that the publisher publishes.

        """
        return self._val.topic_name

    @property
    def construction_order(self) -> int:
        """
        Get a construction order.

        Returns
        -------
        int
            A construction order of this publisher.

        """
        return self._val.construction_order

    @property
    def callback_names(self) -> list[str] | None:
        """
        Get callback names.

        Returns
        -------
        list[str] | None
            Callback names which uses the publisher to publish.

        """
        names = self._val.callback_names
        if names is None:
            return None
        return sorted(names)

    @property
    def qos(self) -> Qos | None:
        """
        Get QoS.

        Returns
        -------
        Qos | None
            Publisher QoS

        """
        if isinstance(self._provider, RuntimeDataProvider):
            return self._provider.get_qos(self._val)
        return None

    @property
    def value(self) -> PublisherStructValue:
        """
        Get StructValue object.

        Returns
        -------
        PublisherStructValue
            publisher value.

        Notes
        -----
        This property is for CARET debugging purposes.

        """
        return self._val

    def _to_records_core(self) -> RecordsInterface:
        """
        Calculate records.

        Returns
        -------
        RecordsInterface
            Publish records.

        """
        records = self._provider.publish_records(self._val)
        return records
