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

from typing import Optional, Sequence

from tracetools_analysis.loading import load_file

from .ros2_tracing.data_model import DataModel
from .ros2_tracing.processor import Ros2Handler
from .value_objects import (PublisherValueLttng,
                            SubscriptionCallbackValueLttng,
                            TimerCallbackValueLttng)
from ..infra_base import InfraBase
from ...record import RecordsInterface
from ...value_objects import CallbackGroupValue, ExecutorValue, NodeValue, NodeValueWithId


class Lttng(InfraBase):
    """
    Lttng data container class.

    This class is a singleton in order to retain information.
    The main processing is done by LttngInfo and RecordsSource.

    """

    _last_load_dir: Optional[str] = None

    def __init__(
        self,
        trace_dir: str,
        force_conversion: bool = False,
        use_singleton_cache: bool = True,
    ) -> None:
        from .lttng_info import LttngInfo
        from .records_source import RecordsSource

        if self._last_load_dir == trace_dir and use_singleton_cache is True:
            return

        Lttng._last_load_dir = trace_dir
        data = self._parse_lttng_data(trace_dir, force_conversion)
        self._records: RecordsSource = RecordsSource(data)
        self._info = LttngInfo(data, self._records)

    def clear_singleton_cache(self) -> None:
        self._last_load_dir = None

    @staticmethod
    def _parse_lttng_data(
        trace_dir: str,
        force_conversion: bool
    ) -> DataModel:
        events = load_file(trace_dir, force_conversion=force_conversion)
        # events = Lttng._filter_runtime_events(events)
        handler = Ros2Handler.process(events)
        return handler.data

    @staticmethod
    def _filter_runtime_events(events):
        runtimes = [
            'ros2:callback_end',
            'ros2:callback_start',
            'ros2:dispatch_intra_process_subscription_callback',
            'ros2:dispatch_subscription_callback',
            'ros2:dds_write',
            'ros2:dds_bind_addr_to_stamp',
            'ros2:rcl_publish',
            'ros2:rclcpp_publish',
        ]
        print(f'{len(events)} events. ')
        events = list(filter(
            lambda x: x['_name'] not in runtimes, events
        ))
        print(f'filtered : {len(events)} events. ')
        return events

    def get_nodes(
        self
    ) -> Sequence[NodeValueWithId]:
        """
        Get nodes.

        Returns
        -------
        Sequence[NodeValueWithId]
            nodes info.

        """
        return self._info.get_nodes()

    def get_rmw_impl(
        self
    ) -> str:
        """
        Get rmw implementation.

        Returns
        -------
        str
            rmw_implementation

        """
        return self._info.get_rmw_impl()

    def get_executors(
        self
    ) -> Sequence[ExecutorValue]:
        """
        Get executors information.

        Returns
        -------
        Sequence[ExecutorInfo]

        """
        return self._info.get_executors()

    def get_callback_groups(
        self,
        node: NodeValue
    ) -> Sequence[CallbackGroupValue]:
        """
        Get callback group information.

        Returns
        -------
        Sequence[CallbackGroupValue]

        """
        return self._info.get_callback_groups(node)

    def get_publishers(
        self,
        node: NodeValue
    ) -> Sequence[PublisherValueLttng]:
        """
        Get publishers information.

        Parameters
        ----------
        node : NodeValue
            target node.

        Returns
        -------
        Sequence[PublisherInfoLttng]

        """
        return self._info.get_publishers(node)

    def get_timer_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[TimerCallbackValueLttng]:
        """
        Get timer callback values.

        Parameters
        ----------
        node : NodeValue
            target node name.

        Returns
        -------
        Sequence[TimerCallbackInfoLttng]

        """
        return self._info.get_timer_callbacks(node)

    def get_subscription_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[SubscriptionCallbackValueLttng]:
        """
        Get subscription callbacks infomation.

        Parameters
        ----------
        node : NodeValue
            target node name.

        Returns
        -------
        Sequence[SubscriptionCallbackInfoLttng]

        """
        return self._info.get_subscription_callbacks(node)

    def compose_inter_proc_comm_records(
        self,
    ) -> RecordsInterface:
        """
        Compose inter process communication records of all communications in one records.

        Returns
        -------
        RecordsInterface
            columns
            - callback_object
            - callback_start_timestamp
            - publisher_handle
            - rclcpp_publish_timestamp
            - rcl_publish_timestamp
            - dds_write_timestamp

        """
        return self._records.compose_inter_proc_comm_records()

    def compose_intra_proc_comm_records(
        self,
    ) -> RecordsInterface:
        """
        Compose intra process communication records of all communications in one records.

        Returns
        -------
        RecordsInterface
            columns:
            - callback_object
            - callback_start_timestamp
            - publisher_handle
            - rclcpp_intra_publish_timestamp

        """
        return self._records.compose_intra_proc_comm_records()

    def compose_callback_records(
        self,
    ) -> RecordsInterface:
        """
        Compose callback records of all communications in one records.

        Returns
        -------
        RecordsInterface
            columns:
            - callback_start_timestamp
            - callback_end_timestamp
            - is_intra_process
            - callback_object

        """
        return self._records.compose_callback_records()
