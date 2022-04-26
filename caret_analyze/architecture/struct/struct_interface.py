from __future__ import annotations

from abc import ABCMeta, abstractmethod
from typing import Any, Iterator, Optional, Sequence, Tuple, Union, List

from caret_analyze.value_objects.transform import TransformValue

from ...value_objects import (
    CallbackStructValue,
    PublisherStructValue,
    SubscriptionStructValue,
    TransformFrameBroadcasterStructValue,
    TransformFrameBufferStructValue,
    VariablePassingStructValue,
)


class PublisherStructInterface(metaclass=ABCMeta):

    @abstractmethod
    def to_value(self) -> PublisherStructValue:
        pass

    @property
    @abstractmethod
    def topic_name(self) -> str:
        pass

    @abstractmethod
    def is_pair(self, other: Any) -> bool:
        pass


class PublishersStructInterface(metaclass=ABCMeta):

    @abstractmethod
    def get(
        self,
        node_name: str,
        topic_name: str
    ) -> PublisherStructInterface:
        pass

    @abstractmethod
    def __iter__(self) -> Iterator[PublisherStructInterface]:
        pass

    def as_list(self) -> List[PublisherStructInterface]:
        return list(self)

    def to_value(self) -> Tuple[PublisherStructValue, ...]:
        return tuple(publisher.to_value() for publisher in self)


class TransformFrameBroadcasterStructInterface(metaclass=ABCMeta):

    @abstractmethod
    def to_value(self) -> TransformFrameBroadcasterStructValue:
        pass

    @property
    @abstractmethod
    def transform(self) -> TransformValue:
        pass

    @abstractmethod
    def is_pair(self, other: Any) -> bool:
        pass

    @property
    def frame_id(self) -> str:
        return self.transform.frame_id

    @property
    def child_frame_id(self) -> str:
        return self.transform.child_frame_id


class TransformFrameBufferStructInterface(metaclass=ABCMeta):

    @property
    @abstractmethod
    def lookup_node_name(self) -> str:
        pass

    @property
    @abstractmethod
    def lookup_transform(self) -> TransformValue:
        pass

    @property
    @abstractmethod
    def listen_transform(self) -> TransformValue:
        pass

    @property
    def listen_frame_id(self) -> str:
        return self.listen_transform.frame_id

    @property
    def listen_child_frame_id(self) -> str:
        return self.listen_transform.child_frame_id

    @property
    def lookup_frame_id(self) -> str:
        return self.lookup_transform.frame_id

    @property
    def lookup_child_frame_id(self) -> str:
        return self.lookup_transform.child_frame_id

    @abstractmethod
    def to_value(self) -> TransformFrameBufferStructValue:
        pass

    @abstractmethod
    def is_pair(self, other: Any) -> bool:
        pass


class SubscriptionStructInterface(metaclass=ABCMeta):

    @property
    @abstractmethod
    def node_name(self) -> str:
        pass

    @property
    @abstractmethod
    def topic_name(self) -> str:
        pass

    @abstractmethod
    def to_value(self) -> SubscriptionStructValue:
        pass

    @abstractmethod
    def is_pair(self, other: Any) -> bool:
        pass


class SubscriptionsStructInterface(metaclass=ABCMeta):

    @abstractmethod
    def get(self, node_name: str, topic_name: str) -> SubscriptionStructInterface:
        pass

    @abstractmethod
    def __iter__(self) -> Iterator[SubscriptionStructInterface]:
        pass

    def to_value(self) -> Tuple[SubscriptionStructValue, ...]:
        return tuple(subscription.to_value() for subscription in self)

    def as_list(self) -> List[SubscriptionStructInterface]:
        return list(self)


NodeOutputType = Union[TransformFrameBroadcasterStructInterface,
                       PublisherStructInterface]
NodeInputType = Union[TransformFrameBufferStructInterface,
                      SubscriptionStructInterface]


class NodeStructsInterface(metaclass=ABCMeta):
    pass


class VariablePassingStructInterface(metaclass=ABCMeta):

    @property
    @abstractmethod
    def callback_name_read(self) -> Optional[str]:
        pass

    @property
    @abstractmethod
    def callback_name_write(self) -> Optional[str]:
        pass

    @abstractmethod
    def to_value(self) -> VariablePassingStructValue:
        pass


class VariablePassingsStructInterface(metaclass=ABCMeta):

    @abstractmethod
    def __iter__(self) -> Iterator[VariablePassingStructInterface]:
        pass

    def to_value(self) -> Tuple[VariablePassingStructValue, ...]:
        return tuple(variable_passing.to_value() for variable_passing in self)


class NodeStructInterface(metaclass=ABCMeta):

    @property
    @abstractmethod
    def node_name(self) -> str:
        pass

    @property
    @abstractmethod
    def callbacks(self) -> Optional[CallbacksStructInterface]:
        pass

    @property
    @abstractmethod
    def variable_passings(self) -> Optional[VariablePassingsStructInterface]:
        pass

    @property
    @abstractmethod
    def node_inputs(self) -> Sequence[NodeInputType]:
        pass

    @property
    @abstractmethod
    def node_outputs(self) -> Sequence[NodeOutputType]:
        pass

    def get_node_input(self) -> NodeInputType:
        raise NotImplementedError('')

    def get_node_output(self) -> NodeOutputType:
        raise NotImplementedError('')

    @property
    @abstractmethod
    def publishers(self) -> Optional[PublishersStructInterface]:
        pass

    @property
    @abstractmethod
    def tf_broadcaster(self) -> Optional[TransformBroadcasterStructInterface]:
        pass

    @property
    @abstractmethod
    def subscriptions(self) -> Optional[SubscriptionsStructInterface]:
        pass

    @property
    @abstractmethod
    def tf_buffer(self) -> Optional[TransformBufferStructInterface]:
        pass


class TransformBroadcasterStructInterface(metaclass=ABCMeta):

    @abstractmethod
    def get(
        self,
        frame_id: str,
        child_frame_id: str
    ) -> TransformFrameBroadcasterStructInterface:
        pass


class TransformBufferStructInterface(metaclass=ABCMeta):

    @abstractmethod
    def get(
        self,
        listen_frame_id: str,
        listen_child_frame_id: str,
        lookup_frame_id: str,
        lookup_child_frame_id: str
    ) -> TransformFrameBufferStructInterface:
        pass


class NodePathStructInterface(metaclass=ABCMeta):

    @property
    @abstractmethod
    def node_input(self) -> Optional[NodeInputType]:
        pass

    @property
    @abstractmethod
    def node_output(self) -> Optional[NodeOutputType]:
        pass

    @property
    @abstractmethod
    def subscription(self) -> Optional[SubscriptionStructInterface]:
        pass

    @property
    @abstractmethod
    def publisher(self) -> Optional[PublisherStructInterface]:
        pass


class NodePathsStructInterface:

    @abstractmethod
    def get(
        self,
        node_name: str,
        node_input: Optional[NodeInputType],
        node_output: Optional[NodeOutputType]
    ) -> NodePathStructInterface:
        pass

    @abstractmethod
    def create(
        self,
        node_name: str,
        node_input: Optional[NodeInputType],
        node_output: Optional[NodeOutputType]
    ) -> None:
        pass


class CallbackStructInterface:

    @property
    @abstractmethod
    def node_inputs(self) -> Optional[Sequence[NodeInputType]]:
        pass

    @property
    @abstractmethod
    def node_outputs(self) -> Optional[Sequence[NodeOutputType]]:
        pass

    @property
    @abstractmethod
    def callback_name(self) -> str:
        pass

    def to_value(self) -> CallbackStructValue:
        pass


class CallbacksStructInterface:

    @abstractmethod
    def get_callback(
        self,
        callback_id: str
    ) -> CallbackStructInterface:
        pass

    @abstractmethod
    def get_callbacks(
        self,
        *callback_ids: str
    ) -> CallbacksStructInterface:
        pass

    def __iter__(self) -> Iterator[CallbackStructInterface]:
        pass

    def as_list(self) -> List[CallbackStructInterface]:
        return list(self)

    def to_value(self) -> Tuple[CallbackStructValue, ...]:
        pass
