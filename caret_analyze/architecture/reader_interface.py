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

from abc import ABCMeta, abstractmethod
from typing import Dict, Optional, Sequence

from functools import lru_cache

from caret_analyze.value_objects.transform import TransformValue

from ..common import Util

from ..value_objects import (
    CallbackGroupValue,
    ClientCallbackValue,
    ExecutorValue,
    NodeValue,
    PathValue,
    PublisherValue,
    ServiceCallbackValue,
    SubscriptionCallbackValue,
    SubscriptionValue,
    TimerCallbackValue,
    TimerValue,
    TransformBroadcasterValue,
    TransformBufferValue,
    VariablePassingValue,
)

IGNORE_TOPICS = ['/parameter_events', '/rosout', '/clock']


class ArchitectureReader(metaclass=ABCMeta):
    """Architecture reader base class."""

    @abstractmethod
    def get_nodes(
        self
    ) -> Sequence[NodeValue]:
        """
        Get nodes.

        Returns
        -------
        Sequence[NodeValue]
            node values.

        """
        pass

    @abstractmethod
    def get_node(
        self,
        node_name: str
    ) -> NodeValue:
        pass

    @abstractmethod
    def get_tf_frames(self) -> Sequence[TransformValue]:
        pass

    def get_timer_callbacks(
        self,
        node_name: str
    ) -> Sequence[TimerCallbackValue]:
        node = self.get_node(node_name)
        return self._get_timer_callbacks(node)

    @abstractmethod
    def _get_timer_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[TimerCallbackValue]:
        """
        Get timer callback values.

        Parameters
        ----------
        node : NodeValue
            target node

        Returns
        -------
        Sequence[TimerCallbackStructInfo]
            timer callback values

        """
        pass

    def get_subscription_callbacks(
        self,
        node_name: str
    ) -> Sequence[SubscriptionCallbackValue]:
        node = self.get_node(node_name)
        return self._get_subscription_callbacks(node)

    def get_service_callbacks(
        self,
        node_name: str
    ) -> Sequence[ServiceCallbackValue]:
        node = self.get_node(node_name)
        return self._get_service_callbacks(node)

    @abstractmethod
    def _get_service_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[ServiceCallbackValue]:
        """
        Get service callback values.

        Parameters
        ----------
        node : NodeValue
            target node

        Returns
        -------
        Sequence[ServiceCallbackStructInfo]
            service callback values

        """
        pass

    def get_client_callbacks(
        self,
        node_name: str
    ) -> Sequence[ClientCallbackValue]:
        node = self.get_node(node_name)
        return self._get_client_callbacks(node)

    @abstractmethod
    def _get_client_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[ClientCallbackValue]:
        """
        Get client callback values.

        Parameters
        ----------
        node : NodeValue
            target node

        Returns
        -------
        Sequence[ClientCallbackStructInfo]
            client callback values

        """
        pass

    @abstractmethod
    def _get_subscription_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[SubscriptionCallbackValue]:
        """
        Get subscription callback values.

        Parameters
        ----------
        node : NodeInfo
            target node

        Returns
        -------
        Sequence[SubscriptionCallbackInfo]
            subscription callback values

        """
        pass

    def get_publishers(
        self,
        node_name: str
    ) -> Sequence[PublisherValue]:
        node = self.get_node(node_name)
        return self._get_publishers(node)

    @abstractmethod
    def _get_publishers(
        self,
        node: NodeValue
    ) -> Sequence[PublisherValue]:
        """
        Get publishers info.

        Parameters
        ----------
        node : NodeValue
            target node

        Returns
        -------
        Sequence[PublisherValue]
            publisher values

        """
        pass

    def get_timers(
        self,
        node_name: str
    ) -> Sequence[TimerValue]:
        node = self.get_node(node_name)
        return self._get_timers(node)

    @abstractmethod
    def _get_timers(
        self,
        node: NodeValue
    ) -> Sequence[TimerValue]:
        """
        Get timers info.

        Parameters
        ----------
        node : NodeValue
            target node

        Returns
        -------
        Sequence[TimerValue]
            timers values

        """
        pass

    def get_subscriptions(
        self,
        node_name: str
    ) -> Sequence[SubscriptionValue]:
        node = self.get_node(node_name)
        return self._get_subscriptions(node)

    @abstractmethod
    def _get_subscriptions(
        self,
        node: NodeValue
    ) -> Sequence[SubscriptionValue]:
        """
        Get subscription values.

        Parameters
        ----------
        node : NodeInfo
            target node

        Returns
        -------
        Sequence[SubscriptionValue]
            subscription values

        """
        pass

    @abstractmethod
    def get_paths(self) -> Sequence[PathValue]:
        """
        Get path value.

        Returns
        -------
        Sequence[PathInfo]
            path values

        """
        pass

    def get_message_contexts(
        self,
        node_name: str
    ) -> Sequence[Dict]:
        node = self.get_node(node_name)
        return self._get_message_contexts(node)

    @abstractmethod
    def _get_message_contexts(
        self,
        node: NodeValue
    ) -> Sequence[Dict]:
        """
        Get message contexts.

        Parameters
        ----------
        node : NodeValue
            target node

        Returns
        -------
        Sequence[Dict]

        """
        pass

    def get_variable_passings(
        self,
        node_name: str
    ) -> Sequence[VariablePassingValue]:
        node = self.get_node(node_name)
        return self._get_variable_passings(node)

    @abstractmethod
    def _get_variable_passings(
        self,
        node: NodeValue
    ) -> Sequence[VariablePassingValue]:
        """
        Get variable passing values.

        Parameters
        ----------
        node : NodeInfo
            target node

        Returns
        -------
        Sequence[VariablePassingvalue]
            variable pssing values

        """
        pass

    @abstractmethod
    def get_executors(self) -> Sequence[ExecutorValue]:
        """
        Get executor values.

        Returns
        -------
        Sequence[ExecutorValue]
            executor values

        """
        pass

    def get_callback_groups(
        self,
        node_name: str
    ) -> Sequence[CallbackGroupValue]:
        node = self.get_node(node_name)
        return self._get_callback_groups(node)
        # @staticmethod
        # def _validate(cbg: CallbackGroupValue, node: NodeValue):
        #     # TODO: add callback group id validation

        #     if len(cbg.callback_ids) != len(set(cbg.callback_ids)):
        #         raise InvalidReaderError(f'duplicated callback id. {node}, {cbg}')

    @abstractmethod
    def _get_callback_groups(
        self,
        node: NodeValue
    ) -> Sequence[CallbackGroupValue]:
        """
        Get callback group values.

        Parameters
        ----------
        node : NodeInfo
            target node

        Returns
        -------
        Sequence[CallbackGroupValue]
            callback group values

        """
        pass

    @lru_cache
    def get_tf_buffer(
        self,
        node_name: str
    ) -> Optional[TransformBufferValue]:
        node = self.get_node(node_name)
        return self._get_tf_buffer(node)

    @abstractmethod
    def _get_tf_buffer(
        self,
        node: NodeValue
    ) -> Optional[TransformBufferValue]:
        pass

    def get_tf_broadcaster(
        self,
        node_name: str
    ) -> Optional[TransformBroadcasterValue]:
        node = self.get_node(node_name)
        return self._get_tf_broadcaster(node)

    @abstractmethod
    def _get_tf_broadcaster(
        self,
        node: NodeValue
    ) -> Optional[TransformBroadcasterValue]:
        pass
