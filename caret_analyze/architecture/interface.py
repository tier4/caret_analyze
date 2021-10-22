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
from typing import List

from ..value_objects.callback_info import SubscriptionCallbackInfo, TimerCallbackInfo
from ..value_objects.path_alias import PathAlias
from ..value_objects.publisher import Publisher
from ..value_objects.variable_passing_info import VariablePassingInfo

UNDEFINED_STR = 'UNDEFINED'
IGNORE_TOPICS = ['/parameter_events', '/rosout', '/clock']


class ArchitectureReader(metaclass=ABCMeta):
    """Architecture reader base class."""

    @abstractmethod
    def get_node_names(
        self
    ) -> List[str]:
        """
        Get node names.

        Returns
        -------
        List[str]
            node names.

        """
        pass

    @abstractmethod
    def get_timer_callbacks(
        self,
        node_name: str
    ) -> List[TimerCallbackInfo]:
        """
        Get timer callbacks info.

        Parameters
        ----------
        node_name : str
            target node name

        Returns
        -------
        List[TimerCallbackInfo]
            timer callback info

        """
        pass

    @abstractmethod
    def get_subscription_callbacks(
        self,
        node_name: str
    ) -> List[SubscriptionCallbackInfo]:
        """
        Get subscription callbacks info.

        Parameters
        ----------
        node_name : str
            target node name

        Returns
        -------
        List[SubscriptionCallbackInfo]
            subscription callback inro

        """
        pass

    @abstractmethod
    def get_publishers(
        self,
        node_name: str
    ) -> List[Publisher]:
        """
        Get publishers info.

        Parameters
        ----------
        node_name : str
            target node name.

        Returns
        -------
        List[PublishInfo]
            publisher info

        """
        pass

    @abstractmethod
    def get_path_aliases(self) -> List[PathAlias]:
        """
        Get callback path alias.

        Returns
        -------
        List[PathAlias]
            path alias

        """
        pass

    @abstractmethod
    def get_variable_passings(
        self,
        node_name: str
    ) -> List[VariablePassingInfo]:
        """
        Get variable passing info.

        Parameters
        ----------
        node_name : str

        Returns
        -------
        List[VariablePassingInfo]

        """

    # @abstractmethod
    # def get_executors(self) -> List[ExecutorInfo]:
    #     """Get executors info.

    #     Returns
    #     -------
    #     List[ExecutorInfo]
    #         executor info

    #     """
    #     pass

    # @abstractmethod
    # def get_callback_groups(
    #     self,
    #     node_name: str
    # ) -> List[CallbackGroupInfo]:
    #     """Get callback groups info.

    #     Parameters
    #     ----------
    #     node_name : str
    #         target node name

    #     Returns
    #     -------
    #     List[CallbackGroupInfo]
    #         callback group info

    #     """
    #     pass
