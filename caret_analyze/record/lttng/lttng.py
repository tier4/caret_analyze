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

from typing import List, Optional

from tracetools_analysis.loading import load_file

from .dataframe_formatter import DataFrameFormatter
from .impl import CallbackImpl
from .impl import PublisherImpl
from .impl import SubscriptionCallbackImpl
from .impl import TimerCallbackImpl
from .processor import Ros2Handler
from .records_formatter import RecordsFormatter
from .util import Ros2DataModelUtil
from .. import ArchitectureInfoContainer
from .. import CallbackInterface
from .. import merge_sequencial
from .. import PublisherInterface
from .. import RecordInterface
from .. import RecordsContainer
from .. import RecordsInterface
from .. import SubscriptionCallbackInterface
from .. import TimerCallbackInterface
from ..record_factory import RecordsFactory
from ...util import Singleton


class Lttng(Singleton, RecordsContainer, ArchitectureInfoContainer):
    load_dir: Optional[str] = None

    def __init__(self, trace_dir, force_conversion: bool = False):
        if Lttng.load_dir == trace_dir:
            return
        Lttng.load_dir = trace_dir
        events = load_file(trace_dir, force_conversion=force_conversion)
        handler = Ros2Handler.process(events)
        data_util = Ros2DataModelUtil(handler.data)

        self._dataframe = DataFrameFormatter(data_util)
        self._records = RecordsFormatter(data_util, self._dataframe)

    def _to_local_callback(self, attr: CallbackInterface) -> CallbackImpl:
        if isinstance(attr, TimerCallbackInterface):
            return TimerCallbackImpl(
                attr.node_name, attr.callback_name, attr.symbol, attr.period_ns, self._dataframe
            )
        elif isinstance(attr, SubscriptionCallbackInterface):
            return SubscriptionCallbackImpl(
                attr.node_name, attr.callback_name, attr.symbol, attr.topic_name, self._dataframe
            )
        assert False, 'Not implemented'

    def get_rmw_implementation(self) -> str:
        return self._dataframe.get_rmw_implementation()

    def get_node_names(self) -> List[str]:
        return self._dataframe.get_node_names()

    def get_publishers(
        self, node_name: str = None, topic_name: str = None
    ) -> List[PublisherInterface]:
        pub_attrs: List[PublisherInterface] = []
        for _, row in self._dataframe.get_publisher_info().iterrows():
            if (node_name is None or node_name == row['name']) and (
                topic_name is None or topic_name == row['topic_name']
            ):
                attr = PublisherImpl(
                    self._dataframe, node_name=row['name'], topic_name=row['topic_name']
                )
                pub_attrs.append(attr)
        return pub_attrs

    def get_timer_callbacks(
        self, node_name: str = None, period_ns: int = None
    ) -> List[TimerCallbackInterface]:
        timer_attrs: list[TimerCallbackInterface] = []
        sort_columns = ['name', 'symbol', 'period_ns']

        timer_info_df = self._dataframe.get_timer_info()
        sorted_df = timer_info_df.sort_values(sort_columns)
        for _, node_timer_info_df in sorted_df.groupby(['name']):
            node_timer_info_df.reset_index(drop=True, inplace=True)
            for idx, row in node_timer_info_df.iterrows():
                if (node_name is None or node_name == row['name']) and (
                    period_ns is None or period_ns == row['period_ns']
                ):
                    callback_name = f'timer_callback_{idx}'
                    attr = TimerCallbackImpl(
                        node_name=row['name'],
                        callback_name=callback_name,
                        symbol=row['symbol'],
                        period_ns=row['period_ns'],
                    )
                    timer_attrs.append(attr)
        return timer_attrs

    def get_subscription_callbacks(
        self, node_name: str = None, topic_name: str = None
    ) -> List[SubscriptionCallbackInterface]:
        attrs: List[SubscriptionCallbackInterface] = []
        # プロセス内とプロセス間で２つのcallback objectが生成される
        group_columns = ['name', 'symbol', 'topic_name']

        sub_info_df = self._dataframe.get_subscription_info()
        sub_trimmed_df = sub_info_df[group_columns].drop_duplicates()
        sorted_df = sub_trimmed_df.sort_values(group_columns)
        for _, node_sub_info_df in sorted_df.groupby(['name']):
            node_sub_info_df.reset_index(drop=True, inplace=True)
            for idx, row in node_sub_info_df.iterrows():
                if (node_name is None or node_name == row['name']) and (
                    topic_name is None or topic_name == row['topic_name']
                ):
                    callback_name = f'subscription_callback_{idx}'
                    attr = SubscriptionCallbackImpl(
                        node_name=row['name'],
                        callback_name=callback_name,
                        symbol=row['symbol'],
                        topic_name=row['topic_name'],
                    )
                    attrs.append(attr)
        return attrs

    def _compose_specific_communication_records(
        self,
        subscription_callback: SubscriptionCallbackImpl,
        publisher_handle: int,
        is_intra_process: bool,
    ) -> RecordsInterface:
        if is_intra_process:
            def is_target(record: RecordInterface):
                # There is no column for the case where an unknown callback is receiving the message.
                if 'callback_object' not in record.columns:
                    return False
                return record.get('publisher_handle') == publisher_handle and \
                    record.get(
                        'callback_object') == subscription_callback.intra_callback_object
        else:
            def is_target(record: RecordInterface):
                # There is no column for the case where an unknown callback is receiving the message.
                if 'callback_object' not in record.columns:
                    return False
                return record.get('publisher_handle') == publisher_handle and \
                    record.get(
                        'callback_object') == subscription_callback.inter_callback_object


        communication_records = self._records.get_communication_records(is_intra_process)
        communication_records_filtered = communication_records.filter_if(is_target)
        assert communication_records_filtered is not None
        return communication_records_filtered

    def _compose_communication_records(
        self,
        subscription_callback: SubscriptionCallbackImpl,
        publish_callback: CallbackImpl,
        is_intra_process: bool,
    ) -> RecordsInterface:

        publisher_handles = self._dataframe.get_publisher_handles(
            subscription_callback.topic_name, publish_callback.node_name
        )

        communication_records = RecordsFactory.create_instance()
        for publisher_handle in publisher_handles:
            communication_records.concat(
                self._compose_specific_communication_records(
                    subscription_callback, publisher_handle, is_intra_process
                ),
                inplace=True,
            )

        return communication_records

    def compose_callback_records(self, callback: CallbackInterface) -> RecordsInterface:
        callback_impl = self._to_local_callback(callback)
        records = self._records.get_callback_records(callback_impl)

        runtime_info_columns = ['callback_object']
        records_dropped = records.drop_columns(runtime_info_columns)
        assert records_dropped is not None
        return records_dropped

    def compose_inter_process_communication_records(
        self,
        subscription_callback: SubscriptionCallbackInterface,
        publish_callback: CallbackInterface,
    ) -> RecordsInterface:
        subscription_callback_impl: SubscriptionCallbackImpl
        publish_callback_impl: CallbackImpl

        subscription_callback_impl = self._to_local_callback(
            subscription_callback)  # type: ignore
        publish_callback_impl = self._to_local_callback(
            publish_callback)  # type: ignore

        records = self._compose_communication_records(
            subscription_callback_impl, publish_callback_impl, is_intra_process=False
        )

        runtime_info_columns = ['callback_object', 'publisher_handle']
        records_dropped = records.drop_columns(runtime_info_columns)
        assert records_dropped is not None
        return records_dropped

    def compose_intra_process_communication_records(
        self,
        subscription_callback: SubscriptionCallbackInterface,
        publish_callback: Optional[CallbackInterface] = None,
    ) -> RecordsInterface:
        subscription_callback_impl: SubscriptionCallbackImpl
        publish_callback_impl: CallbackImpl
        subscription_callback_impl = self._to_local_callback(
            subscription_callback)  # type: ignore
        publish_callback_impl = self._to_local_callback(
            publish_callback)  # type: ignore

        records = self._compose_communication_records(
            subscription_callback_impl,
            publish_callback_impl,
            is_intra_process=True,
        )

        runtime_info_columns = ['callback_object', 'publisher_handle']
        records_dropped = records.drop_columns(runtime_info_columns)
        assert records_dropped is not None
        return records_dropped

    def compose_variable_passing_records(
        self,
        write_callback: CallbackInterface,
        read_callback: CallbackInterface,
    ) -> RecordsInterface:
        write_callback_impl = self._to_local_callback(write_callback)
        read_callback_impl = self._to_local_callback(read_callback)

        read_records = self._records.get_callback_records(
            read_callback_impl).clone()
        read_records.drop_columns(['callback_end_timestamp'], inplace=True)
        read_records.rename_columns(
            {'callback_object': 'read_callback_object'}, inplace=True)

        write_records = self._records.get_callback_records(
            write_callback_impl).clone()
        write_records.rename_columns(
            {'callback_object': 'write_callback_object'}, inplace=True)
        write_records.drop_columns(['callback_start_timestamp'], inplace=True)

        merged_records = merge_sequencial(
            left_records=write_records,
            right_records=read_records,
            left_stamp_key='callback_end_timestamp',
            right_stamp_key='callback_start_timestamp',
            join_key=None,
            how='left',
        )

        merged_records.sort(key='callback_end_timestamp')

        runtime_info_columns = [
            'read_callback_object', 'write_callback_object']
        records_dropped = merged_records.drop_columns(runtime_info_columns)
        assert records_dropped is not None
        return records_dropped
