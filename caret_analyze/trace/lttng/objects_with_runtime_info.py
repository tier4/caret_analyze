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


from typing import Optional, Tuple

from .lttng_info import DataFrameFormatter
from ...exceptions import TraceResultAanalyzeError
from ...value_objects.callback_info import (CallbackInfo,
                                            SubscriptionCallbackInfo,
                                            TimerCallbackInfo)
from ...value_objects.publisher import Publisher
from ...value_objects.subscription import Subscription


class PublisherWithRuntime():

    def __init__(
        self,
        dataframe_formatter: DataFrameFormatter,
        node_name: str,
        topic_name: str
    ) -> None:
        self._publisher = Publisher(node_name, topic_name, None)
        self.publisher_handle = self._get_publisher_handle(
            dataframe_formatter, self._publisher.node_name, self._publisher.topic_name
        )
        assert self.publisher_handle is not None

    @property
    def callback_name(self) -> Optional[str]:
        return self._publisher.callback_name

    @property
    def node_name(self) -> str:
        return self._publisher.node_name

    @property
    def topic_name(self) -> str:
        return self._publisher.topic_name

    def _get_publisher_handle(
        self, dataframe_formatter: DataFrameFormatter, node_name: str, topic_name: str
    ):
        for _, row in dataframe_formatter.get_publisher_info().iterrows():
            if row['name'] != node_name or row['topic_name'] != topic_name:
                continue
            return row['publisher_handle']


# class SubscriptionWithRuntime():

#     def __init__(
#         self,
#         subscription: Subscription
#     ) -> None:
#         self._node_name = node_name
#         self._topic_name = topic_name
#         self._callback_name = callback_name

#     @property
#     def node_name(self) -> str:
#         return self._node_name

#     @property
#     def topic_name(self) -> str:
#         return self._topic_name

#     @property
#     def callback_name(self) -> str:
#         return self._callback_name


class CallbackWithRuntime():

    def __init__(
        self,
        callback_info: CallbackInfo,
    ) -> None:
        self._callback_info = callback_info

    @property
    def node_name(self) -> str:
        return self._callback_info.node_name

    @property
    def symbol(self) -> str:
        return self._callback_info.symbol

    @property
    def callback_name(self) -> str:
        return self._callback_info.callback_name

    @property
    def subscription(self) -> Optional[Subscription]:
        return self._callback_info.subscription


class TimerCallbackWithRuntime(CallbackWithRuntime):

    def __init__(
        self,
        node_name: str,
        callback_name: str,
        symbol: str,
        period_ns: int,
        dataframe_formatter: Optional[DataFrameFormatter] = None,
        *,
        logger=None
    ) -> None:
        self._callback_info = TimerCallbackInfo(
            node_name, callback_name, symbol, period_ns
        )
        super().__init__(self._callback_info)

        if dataframe_formatter is not None:
            self.callback_object = self._get_callback_object(
                self._callback_info.node_name,
                self._callback_info.symbol,
                self._callback_info.period_ns,
                dataframe_formatter
            )
            if self.callback_object is None:
                err_msg = (
                    'Failed to find callback_object.'
                    f'node_name: {self._callback_info.node_name},'
                    f'callback_name: {self._callback_info.callback_name},'
                    f' period_ns: {self._callback_info.period_ns},'
                    f'symbol: {self._callback_info.symbol}'
                )
                raise TraceResultAanalyzeError(err_msg)

    @property
    def period_ns(self) -> int:
        return self._callback_info.period_ns

    def _get_callback_object(
        self, node_name: str, symbol: str, period_ns: int, dataframe_formatter: DataFrameFormatter
    ) -> Optional[int]:
        for _, row in dataframe_formatter.get_timer_info().iterrows():
            if (
                row['name'] != node_name
                or row['symbol'] != symbol
                or row['period_ns'] != period_ns
            ):
                continue
            return row['callback_object']
        return None


class SubscriptionCallbackWithRuntime(CallbackWithRuntime):

    def __init__(
        self,
        node_name: str,
        callback_name: str,
        symbol: str,
        topic_name: str,
        dataframe_formatter: Optional[DataFrameFormatter] = None,
    ) -> None:
        self._callback_info = SubscriptionCallbackInfo(
            node_name, callback_name, symbol, topic_name
        )
        super().__init__(self._callback_info)

        self.intra_callback_object = None
        self.inter_callback_object = None

        if dataframe_formatter is not None:
            (
                self.intra_callback_object,
                self.inter_callback_object,
            ) = self._get_callback_object(self._callback_info, dataframe_formatter)

    @property
    def topic_name(self) -> str:
        return self._callback_info._topic_name

    def _get_callback_object(
        self,
        callback_info: CallbackInfo,
        dataframe_formatter: DataFrameFormatter,
    ) -> Tuple[Optional[int], Optional[int]]:
        group_columns = ['name', 'symbol', 'topic_name']
        sub_info_df = dataframe_formatter.get_subscription_info()

        for key, df in sub_info_df.groupby(group_columns):
            node_name_, symbol_, topic_name_ = key[0], key[1], key[2]

            if node_name_ != callback_info.node_name or \
                symbol_ != callback_info.symbol or \
                    topic_name_ != callback_info.topic_name:
                continue

            if len(df) == 2:
                intra_callback_object = df.iloc[0].callback_object
                inter_callback_object = df.iloc[1].callback_object
                return intra_callback_object, inter_callback_object
            else:
                return None, df.iloc[0].callback_object

        return None, None
