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

from abc import abstractmethod

from typing import (
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
)

from caret_analyze.value_objects.callback import ServiceCallbackValue

from .publisher import PublishersStruct
from .struct_interface import (
    CallbacksStructInterface,
    CallbackStructInterface,
    NodeInputType,
    NodeOutputType,
    PublishersStructInterface,
    SubscriptionStructInterface,
)
from .subscription import SubscriptionsStruct, SubscriptionStruct
from .transform import TransformFrameBroadcasterStruct, TransformFrameBufferStruct
from ...common import Util
from ...exceptions import ItemNotFoundError, UnsupportedTypeError
from ...value_objects import (
    CallbackGroupValue,
    CallbackStructValue,
    CallbackValue,
    NodeValue,
    SubscriptionCallbackStructValue,
    SubscriptionCallbackValue,
    TimerCallbackStructValue,
    TimerCallbackValue,
)


class CallbackStruct(CallbackStructInterface):

    def __init__(
        self,
        callback_name: str,
        callback_id: str,
        node_name: Optional[str] = None,
        symbol: Optional[str] = None,
        subscription: Optional[SubscriptionStructInterface] = None,
        publishers: Optional[PublishersStructInterface] = None,
        tf_buffers: Optional[List[TransformFrameBufferStruct]] = None,
        tf_broadcasters: Optional[List[TransformFrameBroadcasterStruct]] = None,
    ):
        self._node_name = node_name
        self._callback_name = callback_name
        self._callback_id = callback_id
        self._symbol = symbol
        self._sub = subscription
        self._pubs = publishers
        self._tf_buffers = tf_buffers
        self._tf_broadcasters = tf_broadcasters

    @property
    def symbol(self) -> str:
        assert self._symbol is not None
        return self._symbol

    @property
    def callback_id(self) -> str:
        return self._callback_id

    @property
    def callback_name(self) -> str:
        return self._callback_name

    @property
    def node_name(self) -> str:
        assert self._node_name is not None
        return self._node_name

    @property
    def subscribe_topic_name(self) -> Optional[str]:
        if isinstance(self._sub, SubscriptionStruct):
            return self._sub.topic_name
        return None

    @property
    def publish_topic_names(self) -> Optional[List[str]]:
        if isinstance(self._pubs, PublishersStruct):
            return [pub.topic_name for pub in self._pubs]
        return None

    @staticmethod
    def create_instance(
        subscriptions: SubscriptionsStruct,
        publishers: PublishersStruct,
        callback: CallbackValue,
        index: int
    ) -> CallbackStruct:

        indexed = Util.indexed_name(f'{callback.node_name}/callback', index)

        callback_name = callback.callback_name or indexed
        node_name = callback.node_name
        callback_id = callback.callback_id
        sub = None
        if callback.subscribe_topic_name is not None:
            sub = subscriptions.get(node_name, callback.subscribe_topic_name)

        pubs = None
        if callback.publish_topic_names is not None:
            pubs = PublishersStruct()
            for pub_topic_name in callback.publish_topic_names:
                pub = publishers.get(node_name, pub_topic_name)
                pubs.insert(pub)

        if isinstance(callback, TimerCallbackValue):

            return TimerCallbackStruct(
                node_name=node_name,
                symbol=callback.symbol,
                period_ns=callback.period_ns,
                subscription=sub,
                publishers=pubs,
                callback_name=callback_name,
                callback_id=callback_id
            )
        if isinstance(callback, SubscriptionCallbackValue):
            assert callback.subscribe_topic_name is not None
            return SubscriptionCallbackStruct(
                node_name=node_name,
                symbol=callback.symbol,
                subscription=sub,
                publishers=pubs,
                callback_name=callback_name,
                callback_id=callback_id
            )
        if isinstance(callback, ServiceCallbackValue):
            assert callback.service_name is not None
            return ServiceCallbackStruct(
                node_name=node_name,
                callback_id=callback_id,
                callback_name=callback_name,
                symbol=callback.symbol,
            )
        raise UnsupportedTypeError('Unsupported callback type')

    @abstractmethod
    def to_value(self) -> CallbackStructValue:
        pass

    @property
    def node_inputs(self) -> Optional[List[NodeInputType]]:
        inputs: List[NodeInputType] = []
        if self._sub is not None:
            inputs.append(self._sub)
        if self._tf_buffers is not None:
            inputs.extend(self._tf_buffers)

        if inputs == []:
            return None
        return inputs

    @property
    def node_outputs(self) -> Optional[List[NodeOutputType]]:
        outputs: List[NodeOutputType] = []
        if self._pubs is not None:
            outputs.extend(self._pubs.as_list())
        if self._tf_broadcasters is not None:
            outputs.extend(self._tf_broadcasters)

        if outputs == []:
            return None

        return outputs


class TimerCallbackStruct(CallbackStruct):
    def __init__(
        self,
        callback_name: str,
        callback_id: str,
        period_ns: Optional[int] = None,
        node_name: Optional[str] = None,
        symbol: Optional[str] = None,
        subscription: Optional[SubscriptionStructInterface] = None,
        publishers: Optional[PublishersStructInterface] = None,
    ) -> None:
        super().__init__(
            node_name=node_name,
            symbol=symbol,
            subscription=subscription,
            publishers=publishers,
            callback_name=callback_name,
            callback_id=callback_id)

        self._period_ns = period_ns

    @property
    def period_ns(self) -> int:
        assert self._period_ns is not None
        return self._period_ns

    def to_value(self) -> TimerCallbackStructValue:
        publish_topic_names = None if self.publish_topic_names is None \
            else tuple(self.publish_topic_names)

        return TimerCallbackStructValue(
            node_name=self.node_name,
            symbol=self.symbol,
            period_ns=self.period_ns,
            publish_topic_names=publish_topic_names,
            callback_name=self.callback_name,
            callback_id=self.callback_id
        )


class SubscriptionCallbackStruct(CallbackStruct):

    def __init__(
        self,
        callback_name: str,
        callback_id: str,
        node_name: Optional[str] = None,
        symbol: Optional[str] = None,
        subscription: Optional[SubscriptionStructInterface] = None,
        publishers: Optional[PublishersStructInterface] = None,
    ) -> None:
        super().__init__(
            node_name=node_name,
            symbol=symbol,
            subscription=subscription,
            publishers=publishers,
            callback_name=callback_name,
            callback_id=callback_id)
        self.__subscription = subscription

    @property
    def subscribe_topic_name(self) -> str:
        assert self.__subscription is not None
        return self.__subscription.topic_name

    def to_value(self) -> SubscriptionCallbackStructValue:
        publish_topic_names = None if self.publish_topic_names is None \
            else tuple(self.publish_topic_names)

        return SubscriptionCallbackStructValue(
            node_name=self.node_name,
            symbol=self.symbol,
            subscribe_topic_name=self.subscribe_topic_name,
            publish_topic_names=publish_topic_names,
            callback_name=self.callback_name,
            callback_id=self.callback_id
        )


class ServiceCallbackStruct(CallbackStruct):
    def __init__(
        self,
        callback_name: str,
        callback_id: str,
        node_name: Optional[str] = None,
        symbol: Optional[str] = None,
    ) -> None:
        super().__init__(
            node_name=node_name,
            symbol=symbol,
            callback_name=callback_name,
            callback_id=callback_id)

    def to_value(self):
        raise NotImplementedError('')


class CallbacksStruct(CallbacksStructInterface, Iterable):

    def __init__(
        self,
        node: Optional[NodeValue] = None
    ) -> None:
        self._node = node
        self._cb_dict: Dict[str, CallbackStruct] = {}

    @property
    def node(self) -> NodeValue:
        assert self._node is not None
        return self._node

    def insert(self, callback: CallbackStruct) -> None:
        self._cb_dict[callback.callback_id] = callback

    def add(self, callbacks: CallbacksStruct) -> None:
        for callback in callbacks:
            self.insert(callback)
        return None

    @property
    def node_name(self) -> str:
        assert self.node.node_name is not None
        return self.node.node_name

    def to_value(self) -> Tuple[CallbackStructValue, ...]:
        return tuple(_.to_value()
                     for _
                     in self._cb_dict.values()
                     if not isinstance(_, ServiceCallbackStruct))

    # def _validate(self, callbacks: List[CallbackValue]) -> None:
    #     # check node name
    #     for callback in callbacks:
    #         if callback.node_id != self._node.node_id:
    #             msg = 'reader returns invalid callback value. '
    #             msg += f'get [{self._node.node_id}] value returns [{callback.node_id}]'
    #             raise InvalidReaderError(msg)

    #     # check callback name
    #     cb_names: List[str] = [
    #         cb.callback_name for cb in callbacks if cb.callback_name is not None]
    #     if len(cb_names) != len(set(cb_names)):
    #         msg = f'Duplicated callback names. node_name: {self._node.node_name}\n'
    #         for name in set(cb_names):
    #             if cb_names.count(name) >= 2:
    #                 msg += f'callback name: {name} \n'
    #         raise InvalidReaderError(msg)

    #     # check callback id
    #     cb_ids: List[str] = [
    #         cb.callback_id
    #         for cb
    #         in callbacks
    #         if cb.callback_id is not None
    #     ]
    #     if len(cb_names) != len(set(cb_names)):
    #         msg = f'Duplicated callback id. node_name: {self._node.node_name}\n'
    #         for cb_id in set(cb_ids):
    #             if cb_ids.count(cb_id) >= 2:
    #                 msg += f'callback id: {cb_id} \n'
    #         raise InvalidReaderError(msg)

    def get_callback_by_cbg(
        self,
        callback_group: CallbackGroupValue
    ) -> CallbacksStruct:
        callback_structs = CallbacksStruct()

        for callback_id in callback_group.callback_ids:
            # Ensure that the callback information exists.
            callback_struct = self.get_callback(callback_id)

            if isinstance(callback_struct, ServiceCallbackStruct):
                # ServiceCallbackStruct is not supported.
                continue

            callback_structs.insert(callback_struct)

        return callback_structs

    def get_callback(
        self,
        callback_id: str
    ) -> CallbackStruct:
        if callback_id in self._cb_dict:
            return self._cb_dict[callback_id]

        raise ItemNotFoundError(
            'Failed to find callback. '
            f'callback_id: {callback_id}, '
        )

    def get_callbacks(
        self,
        *callback_ids: str
    ) -> CallbacksStruct:
        """
        Get callbacks.

        Parameters
        ----------
        callback_ids : Tuple[str, ...]
            target callback ids

        Returns
        -------
        CallbacksStruct
            If the callback is not found, it returns an empty tuple.

        """
        callbacks = CallbacksStruct()

        for callback_id in callback_ids:
            if callback_id not in self._cb_dict.keys():
                continue
            callback = self.get_callback(callback_id)
            callbacks.insert(callback)

        return callbacks

    def __iter__(self) -> Iterator[CallbackStruct]:
        return iter(self._cb_dict.values())
