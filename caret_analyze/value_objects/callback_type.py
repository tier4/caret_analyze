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


class CallbackType(ValueObject):
    """callback group type class."""

    Timer: CallbackType
    Subscription: CallbackType

    def __init__(self, name: str) -> None:
        """
        Construct callback type.

        Parameters
        ----------
        name : str
            callback type name ['timer_callback', 'subscription_callback']

        """
        if name not in ['timer_callback', 'subscription_callback']:
            raise ValueError(f'Unsupported callback type: {name}')

        self._name = name

    @property
    def name(self) -> str:
        """
        Return callback type name.

        Returns
        -------
        str
            type name.

        """
        return self._name


CallbackType.Timer = CallbackType('timer_callback')
CallbackType.Subscription = CallbackType('subscription_callback')
