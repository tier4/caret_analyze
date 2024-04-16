# Copyright 2021 TIER IV, Inc.
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

"""
Package defining the value object class.

Defined classes are used as an interface across multiple packages:
architecture, infra, and runtime.
CARET uses value objects to avoid unexpectedly overwritten.

"""

from .callback import (CallbackStructValue,
                       CallbackType,
                       CallbackValue,
                       ServiceCallbackStructValue,
                       ServiceCallbackValue,
                       SubscriptionCallbackStructValue,
                       SubscriptionCallbackValue,
                       TimerCallbackStructValue,
                       TimerCallbackValue)
from .callback_group import CallbackGroupStructValue, CallbackGroupType, CallbackGroupValue
from .communication import CommunicationStructValue
from .executor import ExecutorStructValue, ExecutorType, ExecutorValue
from .message_context import (CallbackChain,
                              InheritUniqueStamp,
                              MessageContext,
                              MessageContextType,
                              Tilde,
                              UseLatestMessage)
from .node import DiffNode, NodeStructValue, NodeValue, NodeValueWithId
from .node_path import NodePathStructValue, NodePathValue
from .path import PathStructValue, PathValue
from .publisher import PublisherStructValue, PublisherValue
from .qos import Qos
from .service import ServiceStructValue, ServiceValue
from .subscription import SubscriptionStructValue, SubscriptionValue
from .timer import TimerStructValue, TimerValue
from .value_object import ValueObject
from .variable_passing import VariablePassingStructValue, VariablePassingValue


__all__ = [
    'CallbackChain',
    'CallbackGroupStructValue',
    'CallbackGroupType',
    'CallbackGroupValue',
    'CallbackStructValue',
    'CallbackType',
    'CallbackValue',
    'CommunicationStructValue',
    'DiffNode',
    'ExecutorStructValue',
    'ExecutorType',
    'ExecutorValue',
    'InheritUniqueStamp',
    'MessageContext',
    'MessageContextType',
    'NodePathStructValue',
    'NodePathValue',
    'NodeStructValue',
    'NodeValue',
    'NodeValueWithId',
    'PathStructValue',
    'PathValue',
    'PublisherStructValue',
    'PublisherValue',
    'Qos',
    'SubscriptionCallbackStructValue',
    'SubscriptionCallbackValue',
    'SubscriptionStructValue',
    'SubscriptionValue',
    'ServiceCallbackStructValue',
    'ServiceCallbackValue',
    'ServiceStructValue',
    'ServiceValue',
    'Tilde',
    'TimerValue',
    'TimerStructValue',
    'TimerCallbackStructValue',
    'TimerCallbackValue',
    'UseLatestMessage',
    'ValueObject',
    'VariablePassingStructValue',
    'VariablePassingValue',
]
