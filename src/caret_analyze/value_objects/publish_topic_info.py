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

from .value_object import ValueObject


class PublishTopicInfoValue(ValueObject):
    """PublishTopic info."""

    def __init__(self, topic_name: str, construction_order: int):
        """
        Construct an instance.

        Parameters
        ----------
        topic_name : str
            Topic name.
        construction_order : int
            Construction order of the publisher.

        """
        self._topic_name = topic_name
        self._construction_order = construction_order

    @property
    def topic_name(self) -> str:
        """
        Get topic name.

        Returns
        -------
        str
            Topic name.

        """
        return self._topic_name

    @property
    def construction_order(self) -> int:
        """
        Get construction order.

        Returns
        -------
        int
            Construction order.

        """
        return self._construction_order
