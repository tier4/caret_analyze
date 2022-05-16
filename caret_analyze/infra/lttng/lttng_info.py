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

from functools import cached_property, lru_cache
from logging import getLogger
from typing import Callable, Dict, List, Optional, Sequence, Union

from caret_analyze.infra.lttng.value_objects.timer_control import TimerInit
from caret_analyze.value_objects.timer import TimerValue

import numpy as np
import pandas as pd

from .ros2_tracing.data_model import Ros2DataModel
from .value_objects import (CallbackGroupValueLttng, NodeValueLttng,
                            PublisherValueLttng,
                            SubscriptionCallbackValueLttng,
                            TimerCallbackValueLttng,
                            TimerControl)
from ...common import Util
from ...exceptions import InvalidArgumentError
from ...value_objects import ExecutorValue, NodeValue, Qos

logger = getLogger(__name__)


class LttngInfo:

    def __init__(self, data: Ros2DataModel):
        self._formatted = DataFrameFormatted(data)

        # TODO(hsgwa): check rmw_impl for each process.
        self._rmw_implementation = data.rmw_impl.iloc[0, 0] if len(data.rmw_impl) > 0 else ''
        # self._source = records_source
        # self._binder_cache: Dict[str, PublisherBinder] = {}

        self._timer_cb_cache: Dict[str, Sequence[TimerCallbackValueLttng]] = {}
        self._sub_cb_cache: Dict[str, List[SubscriptionCallbackValueLttng]] = {}
        self._pub_cache: Dict[str, List[PublisherValueLttng]] = {}
        self._cbg_cache: Dict[str, List[CallbackGroupValueLttng]] = {}

        self._id_to_topic: Dict[str, str] = {}
        self._sub_cb_cache_without_pub: Optional[Dict[str, List[SubscriptionCallbackValueLttng]]]
        self._sub_cb_cache_without_pub = None

        self._timer_cb_cache_without_pub: Optional[Dict[str, List[TimerCallbackValueLttng]]]
        self._timer_cb_cache_without_pub = None

    def _get_timer_cbs_without_pub(self, node_id: str) -> List[TimerCallbackValueLttng]:
        if self._timer_cb_cache_without_pub is None:
            self._timer_cb_cache_without_pub = self._load_timer_cbs_without_pub()

        if node_id not in self._timer_cb_cache_without_pub:
            return []

        return self._timer_cb_cache_without_pub[node_id]

    def _get_sub_cbs_without_pub(self, node_id: str) -> List[SubscriptionCallbackValueLttng]:
        if self._sub_cb_cache_without_pub is None:
            self._sub_cb_cache_without_pub = self._load_sub_cbs_without_pub()

        if node_id not in self._sub_cb_cache_without_pub:
            return []

        return self._sub_cb_cache_without_pub[node_id]

    def get_rmw_impl(self) -> str:
        """
        Get rmw implementation.

        Returns
        -------
        str
            rmw_implementation

        """
        return self._rmw_implementation

    def _load_timer_cbs_without_pub(self) -> Dict[str, List[TimerCallbackValueLttng]]:
        timer_cbs_info: Dict[str, List[TimerCallbackValueLttng]] = {}

        for node in self.get_nodes():
            timer_cbs_info[node.node_id] = []

        timer_df = self._formatted.timer_callbacks_df
        timer_df = merge(timer_df, self._formatted.nodes_df, 'node_handle')

        for _, row in timer_df.iterrows():
            node_name = row['node_name']
            node_id = row['node_id']
            timer_cbs_info[node_id].append(
                TimerCallbackValueLttng(
                    callback_id=row['callback_id'],
                    node_name=node_name,
                    node_id=row['node_id'],
                    symbol=row['symbol'],
                    period_ns=row['period_ns'],
                    timer_handle=row['timer_handle'],
                    publish_topic_names=None,
                    callback_object=row['callback_object']
                )
            )

        return timer_cbs_info

    def _get_timer_callbacks(self, node: NodeValue) -> Sequence[TimerCallbackValueLttng]:
        node_id = node.node_id
        assert node_id is not None

        timer_cbs = self._get_timer_cbs_without_pub(node_id)

        # if node_id not in self._binder_cache.keys():
        #     self._binder_cache[node_id] = PublisherBinder(self, self._source)

        # binder = self._binder_cache[node_id]
        # if binder.can_bind(node) and len(timer_cbs) > 0:
        #     timer_cbs = binder.bind_pub_topics_and_timer_cbs(node_id, timer_cbs)

        return timer_cbs

    def get_timer_callbacks(self, node: NodeValue) -> Sequence[TimerCallbackValueLttng]:
        """
        Get timer callbacks information.

        Parameters
        ----------
        node_name : str
            target node name.

        Returns
        -------
        Sequence[TimerCallbackInfo]

        """
        def get_timer_cb_local(node: NodeValueLttng):
            node_id = node.node_id
            if node.node_id not in self._timer_cb_cache.keys():
                self._timer_cb_cache[node_id] = self._get_timer_callbacks(node)
            return self._timer_cb_cache[node_id]

        if node.node_id is None:
            return Util.flatten([
                get_timer_cb_local(node)
                for node
                in self._get_nodes(node.node_name)
            ])

        node_lttng = NodeValueLttng(node.node_name, node.node_id)
        return get_timer_cb_local(node_lttng)

    @lru_cache
    def get_nodes(self) -> Sequence[NodeValueLttng]:
        """
        Get node name list.

        Returns
        -------
        Sequence[NodeValue]
            node names.

        """
        nodes_df = self._formatted.nodes_df

        nodes = []
        added_nodes = set()
        duplicate_nodes = set()

        for _, row in nodes_df.iterrows():
            node_name = row['node_name']
            node_id = row['node_id']
            if node_name in added_nodes:
                duplicate_nodes.add(node_name)
            added_nodes.add(node_name)
            nodes.append(NodeValueLttng(node_name, node_id))

        for duplicate_node in duplicate_nodes:
            logger.warning(
                f'Duplicate node. node_name = {duplicate_node}. '
                'The measurement results may be incorrect.')

        return nodes

    def _load_sub_cbs_without_pub(
        self
    ) -> Dict[str, List[SubscriptionCallbackValueLttng]]:
        sub_cbs_info: Dict[str, List[SubscriptionCallbackValueLttng]] = {}

        for node in self.get_nodes():
            sub_cbs_info[node.node_id] = []

        sub_df = self._formatted.subscription_callbacks_df
        sub_df = merge(sub_df, self._formatted.nodes_df, 'node_handle')
        tilde_sub = self._formatted.tilde_subscriptions_df
        sub_df = pd.merge(sub_df, tilde_sub, on=['node_name', 'topic_name'], how='left')
        sub_df = sub_df.astype({'tilde_subscription': 'Int64'})

        for _, row in sub_df.iterrows():
            node_name = row['node_name']
            node_id = row['node_id']
            tilde_subscription = row['tilde_subscription']
            if tilde_subscription is pd.NA:
                tilde_subscription = None

            # Since callback_object_intra contains nan, it is of type np.float.
            record_callback_object_intra = row['callback_object_intra']
            if record_callback_object_intra is pd.NA:
                callback_object_intra = None
            else:
                callback_object_intra = int(record_callback_object_intra)
            self._id_to_topic[row['callback_id']] = row['topic_name']

            sub_cbs_info[node_id].append(
                SubscriptionCallbackValueLttng(
                    callback_id=row['callback_id'],
                    node_id=node_id,
                    node_name=node_name,
                    symbol=row['symbol'],
                    subscribe_topic_name=row['topic_name'],
                    publish_topic_names=None,
                    subscription_handle=row['subscription_handle'],
                    callback_object=row['callback_object'],
                    callback_object_intra=callback_object_intra,
                    tilde_subscription=tilde_subscription
                )
            )
        return sub_cbs_info

    def _get_subscription_callback_values(
        self,
        node: NodeValue
    ) -> List[SubscriptionCallbackValueLttng]:
        node_id = node.node_id
        assert node_id is not None

        sub_cbs_info: List[SubscriptionCallbackValueLttng]
        sub_cbs_info = self._get_sub_cbs_without_pub(node_id)

        # if node_id not in self._binder_cache.keys():
        #     self._binder_cache[node_id] = PublisherBinder(self, self._source)

        # binder = self._binder_cache[node_id]
        # if binder.can_bind(node):
        #     sub_cbs_info = binder.bind_pub_topics_and_sub_cbs(node_id, sub_cbs_info)

        return sub_cbs_info

    def get_subscription_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[SubscriptionCallbackValueLttng]:
        """
        Get subscription callbacks infomation.

        Parameters
        ----------
        node_name : str
            target node name.

        Returns
        -------
        Sequence[SubscriptionCallbackInfo]

        """
        def get_sub_cb_local(node: NodeValueLttng):
            node_id = node.node_id
            if node_id not in self._sub_cb_cache.keys():
                self._sub_cb_cache[node_id] = self._get_subscription_callback_values(node)
            return self._sub_cb_cache[node_id]

        if node.node_id is None:
            return Util.flatten([
                get_sub_cb_local(node)
                for node
                in self._get_nodes(node.node_name)
            ])

        node_lttng = NodeValueLttng(node.node_name, node.node_id)
        return get_sub_cb_local(node_lttng)

    @property
    def tilde_sub_id_map(self) -> Dict[int, int]:
        return self._formatted.tilde_sub_id_map

    def _get_publishers(self, node: NodeValueLttng) -> List[PublisherValueLttng]:
        node_id = node.node_id
        # if node_id not in self._binder_cache.keys():
        #     self._binder_cache[node_id] = PublisherBinder(self, self._source)

        # binder = self._binder_cache[node_id]
        # if not binder.can_bind(node):
        return self.get_publishers_without_cb_bind(node_id)

        cbs: List[Union[TimerCallbackValueLttng,
                        SubscriptionCallbackValueLttng]] = []
        cbs += self.get_timer_callbacks(node)
        cbs += self.get_subscription_callbacks(node)
        pubs_info = self.get_publishers_without_cb_bind(node_id)

        for i, pub_info in enumerate(pubs_info):
            topic_name = pub_info.topic_name
            cbs_pubs = Util.filter_items(
                lambda x: topic_name in x.publish_topic_names, cbs)
            cb_ids = tuple(c.callback_id for c in cbs_pubs)
            pubs_info[i] = PublisherValueLttng(
                node_name=pub_info.node_name,
                node_id=pub_info.node_id,
                topic_name=pub_info.topic_name,
                callback_ids=cb_ids,
                publisher_handle=pub_info.publisher_handle
            )

        return pubs_info

    def get_publishers(self, node: NodeValue) -> List[PublisherValueLttng]:
        """
        Get publishers information.

        Parameters
        ----------
        node: NodeValue
            target node.

        Returns
        -------
        List[PublisherInfo]

        """
        def get_publishers_local(node: NodeValueLttng):
            node_id = node.node_id

            if node_id not in self._pub_cache.keys():
                self._pub_cache[node_id] = self._get_publishers(node)

            return self._pub_cache[node_id]

        if node.node_id is None:
            return Util.flatten([
                get_publishers_local(node)
                for node
                in self._get_nodes(node.node_name)
            ])

        node_lttng = NodeValueLttng(node.node_name, node.node_id)
        return get_publishers_local(node_lttng)

    def _get_nodes(
        self,
        node_name: str
    ) -> Sequence[NodeValueLttng]:
        return Util.filter_items(lambda x: x.node_name == node_name, self.get_nodes())

    def get_publishers_without_cb_bind(self, node_id: str) -> List[PublisherValueLttng]:
        """
        Get publishers information.

        Parameters
        ----------
        node_name : str
            target node name.

        Returns
        -------
        List[PublisherInfo]

        """
        pub_df = self._formatted.publishers_df
        pub_df = merge(pub_df, self._formatted.nodes_df, 'node_handle')
        tilde_pub = self._formatted.tilde_publishers_df

        pub_df = pd.merge(pub_df, tilde_pub, on=['node_name', 'topic_name'], how='left')
        pub_df = pub_df.astype({'tilde_publisher': 'Int64'})
        pubs_info = []
        for _, row in pub_df.iterrows():
            if row['node_id'] != node_id:
                continue
            tilde_publisher = row['tilde_publisher']
            if tilde_publisher is pd.NA:
                tilde_publisher = None

            pubs_info.append(
                PublisherValueLttng(
                    node_name=row['node_name'],
                    topic_name=row['topic_name'],
                    node_id=row['node_id'],
                    callback_ids=None,
                    publisher_handle=row['publisher_handle'],
                    tilde_publisher=tilde_publisher
                )
            )

        return pubs_info

    def _is_user_made_callback(
        self,
        callback_id: str
    ) -> bool:
        is_subscription = callback_id in self._id_to_topic.keys()
        if not is_subscription:
            return True
        topic_name = self._id_to_topic[callback_id]
        return topic_name not in ['/clock', '/parameter_events']

    def _get_callback_groups(
        self,
        node_id: str
    ) -> List[CallbackGroupValueLttng]:
        concate_target_dfs = []
        concate_target_dfs.append(self._formatted.timer_callbacks_df)
        concate_target_dfs.append(self._formatted.subscription_callbacks_df)

        try:
            column_names = [
                'callback_group_addr', 'callback_id', 'node_handle'
            ]
            concat_df = concat(column_names, concate_target_dfs)

            concat_df = merge(
                concat_df, self._formatted.nodes_df, 'node_handle')
            concat_df = merge(
                concat_df, self._formatted.callback_groups_df, 'callback_group_addr')

            cbgs = []
            for _, group_df in concat_df.groupby(['callback_group_addr']):
                row = group_df.iloc[0, :]
                node_id_ = row['node_id']
                if node_id != node_id_:
                    continue

                callback_ids = tuple(group_df['callback_id'].values)
                callback_ids = tuple(Util.filter_items(self._is_user_made_callback, callback_ids))

                cbgs.append(
                    CallbackGroupValueLttng(
                        callback_group_type_name=row['group_type_name'],
                        node_name=row['node_name'],
                        node_id=node_id,
                        callback_ids=callback_ids,
                        callback_group_id=row['callback_group_id'],
                        callback_group_addr=row['callback_group_addr'],
                        executor_addr=row['executor_addr'],
                    )
                )

            return cbgs
        except KeyError:
            return []

    def get_callback_groups(
        self,
        node: NodeValue
    ) -> Sequence[CallbackGroupValueLttng]:
        """
        Get callback groups value.

        Returns
        -------
        List[CallbackGroupInfo]

        """
        def get_cbg_local(node: NodeValueLttng):
            node_id = node.node_id

            if node_id not in self._cbg_cache:
                self._cbg_cache[node_id] = self._get_callback_groups(node.node_id)

            return self._cbg_cache[node_id]

        if node.node_id is None:
            return Util.flatten([
                get_cbg_local(node)
                for node
                in self._get_nodes(node.node_name)
            ])

        node_lttng = NodeValueLttng(node.node_name, node.node_id)
        return get_cbg_local(node_lttng)

    def get_executors(self) -> List[ExecutorValue]:
        """
        Get executors information.

        Returns
        -------
        List[ExecutorInfo]

        """
        exec_df = self._formatted.executor_df
        cbg_df = self._formatted.callback_groups_df
        exec_df = merge(exec_df, cbg_df, 'executor_addr')
        execs = []

        for _, group in exec_df.groupby('executor_addr'):
            row = group.iloc[0, :]
            executor_type_name = row['executor_type_name']

            cbg_ids = group['callback_group_id'].values
            execs.append(
                ExecutorValue(
                    executor_type_name,
                    tuple(cbg_ids))
            )

        return execs

    def get_publisher_qos(self, publisher: PublisherValueLttng) -> Qos:
        df = self._formatted.publishers_df
        pub_df = df[df['publisher_handle'] == publisher.publisher_handle]

        if len(pub_df) == 0:
            raise InvalidArgumentError('No publisher matching the criteria was found.')
        if len(pub_df) > 1:
            logger.warning(
                'Multiple publishers matching your criteria were found.'
                'The value of the first publisher qos will be returned.')

        depth = int(pub_df['depth'].values[0])
        return Qos(depth)

    def get_subscription_qos(self, callback: SubscriptionCallbackValueLttng) -> Qos:
        df = self._formatted.subscription_callbacks_df
        sub_df = df[df['callback_object'] == callback.callback_object]

        if len(sub_df) == 0:
            raise InvalidArgumentError('No subscription matching the criteria was found.')
        if len(sub_df) > 1:
            logger.warning(
                'Multiple publishers matching your criteria were found.'
                'The value of the first publisher qos will be returned.')

        depth = int(sub_df['depth'].values[0])
        return Qos(depth)

    def get_timers(self, node: NodeValue) -> Sequence[TimerValue]:
        try:
            callbacks = self.get_timer_callbacks(node)
            timers = []
            for callback in callbacks:
                timers.append(
                        TimerValue(
                            period=callback.period_ns,
                            node_name=callback.node_name,
                            node_id=callback.node_id,
                            callback_id=callback.callback_id,
                        )
                    )

            return timers
        except ValueError:
            return []

    def get_timer_controls(self) -> Sequence[TimerControl]:
        df = self._formatted.timer_controls_df
        ctrls: List[TimerControl] = []
        for _, row in df.iterrows():
            if row['type'] == 'init':
                params = row['params']
                ctrl = TimerInit(row['timer_handle'], row['timestamp'], params['period'])
                ctrls.append(ctrl)
            else:
                raise NotImplementedError('Unsupported timer control type.')

        return ctrls


# class PublisherBinder:
#     TARGET_RECORD_MAX_INDEX = 10

#     def __init__(self, lttng_info: LttngInfo, records_source: RecordsSource) -> None:
#         self._info = lttng_info
#         self._source = records_source
#         self._callback_records_cache: Optional[RecordsInterface] = None
#         self._intra_comm_records_cache: Optional[RecordsInterface] = None
#         self._inter_comm_records_cache: Optional[RecordsInterface] = None

#     def can_bind(self, node: NodeValue) -> bool:
#         """
#         If all callbacks in a node are exclusive, the publisher can be tied to the callback.

#         Parameters
#         ----------
#         node_name : str
#             [description]

#         Returns
#         -------
#         bool

#         """
#         # implementation is mostly done, but the processing time is huge, so it is not practical.
#         # Disable it until the speedup is complete.
#         return False

#         cbgs: Sequence[CallbackGroupValueLttng]
#         cbgs = self._info.get_callback_groups(node)

#         # TODO: ignore /parameter_events, /clock
#         # Ignore callback groups that have no callbacks added,
#         # as they are irrelevant to performance.
#         # if len(callback_ids) == 0:
#         #     self._ignored_callback_groups.add(row['callback_group_id'])
#         #     continue

#         if len(cbgs) != 1:
#             print('false')
#             return False

#         cbg = cbgs[0]
#         if cbg.callback_group_type is CallbackGroupType.REENTRANT:
#             print('false')
#             return False

#         print('true')
#         return True

#     def bind_pub_topics_and_timer_cbs(
#         self,
#         node_name: str,
#         callbacks: Sequence[TimerCallbackValueLttng],
#     ) -> List[TimerCallbackValueLttng]:
#         """
#         Return publisher binded callback values.

#         Note:
#         This function call takes a long time because binding uses records.

#         Parameters
#         ----------
#         node_name : str
#         callbacks_info : Sequence[TimerCallbackValueLttng]

#         Returns
#         -------
#         List[TimerCallbackValueLttng]
#             publisher binded callback values.

#         """
#         callback_list: List[TimerCallbackValueLttng]
#         callback_list = list(callbacks)

#         # insert empty tuple
#         for cb in callbacks:
#             self._update_timer_cb_publish_topics(callback_list, cb, ())

#         publishers = self._info.get_publishers_without_cb_bind(node_name)
#         publishers = Util.filter_items(
#             lambda x: x.topic_name not in ['/parameter_events', '/rosout'],
#             publishers
#         )

#         from itertools import product

#         from tqdm import tqdm

#         it = list(product(publishers, callbacks))
#         for publisher, cb in tqdm(it):
#             if not self._is_consistent(publisher, cb):
#                 continue

#             topic_names: Tuple[str, ...] = (publisher.topic_name,)
#             if cb.publish_topic_names is not None:
#                 topic_names = topic_names + cb.publish_topic_names

#             self._update_timer_cb_publish_topics(
#                 callback_list, cb, topic_names)
#             break

#         return callback_list

#     def bind_pub_topics_and_sub_cbs(
#         self,
#         node_name: str,
#         callbacks_info: Sequence[SubscriptionCallbackValueLttng],
#     ) -> List[SubscriptionCallbackValueLttng]:
#         """
#         Return publisher binded callback values.

#         Note:
#         This function call takes a long time because binding uses records.

#         Parameters
#         ----------
#         node_name : str
#         callbacks_info : Sequence[SubscriptionCallbackValueLttng]

#         Returns
#         -------
#         List[SubscriptionCallbackValueLttng]
#             publisher binded callback values.

#         """
#         system_topics = ['/parameter_events', '/rosout', '/clock']
#         callback_list: List[SubscriptionCallbackValueLttng]
#         callback_list = list(callbacks_info)
#         callback_list = Util.filter_items(
#             lambda x: x.subscribe_topic_name not in system_topics, callback_list)

#         # insert empty tuple
#         for cb_info in callback_list:
#             self._update_sub_cb_publish_topics(callback_list, cb_info, ())

#         publishers = self._info.get_publishers_without_cb_bind(node_name)
#         publishers = Util.filter_items(lambda x: x.topic_name not in system_topics, publishers)

#         from itertools import product

#         from tqdm import tqdm

#         it = list(product(publishers, callback_list))
#         for publisher, cb_info in tqdm(it):
#             if not self._is_consistent(publisher, cb_info):
#                 continue
#             topic_names: Tuple[str, ...] = (publisher.topic_name,)
#             if cb_info.publish_topic_names is not None:
#                 topic_names = topic_names + cb_info.publish_topic_names

#             self._update_sub_cb_publish_topics(
#                 callback_list, cb_info, topic_names)
#             break

#         return callback_list

#     def _get_publish_time(
#         self,
#         publisher_info: PublisherValueLttng
#     ) -> Optional[int]:
#         def select_record_index(records: RecordsInterface) -> int:
#             # Select publish after the initialization is complete.
#             # To reduce the search time from the beginning. The smaller the index, the better.
#             # Note: intra_porocess cyclic demo is manually publishing the first message.
#             return min(len(publisher_records.data)-1, self.TARGET_RECORD_MAX_INDEX)

#         publisher_handle = publisher_info.publisher_handle

#         publisher_records = self._source.inter_proc_comm_records.clone()
#         publisher_records.filter_if(lambda x: x.get(
#             'publisher_handle') == publisher_handle)
#         if len(publisher_records) > 0:
#             publish_index = select_record_index(publisher_records)
#             return publisher_records.data[publish_index].get('rclcpp_publish_timestamp')

#         publisher_records = self._source.intra_proc_comm_records.clone()
#         publisher_records.filter_if(lambda x: x.get(
#             'publisher_handle') == publisher_handle)
#         if len(publisher_records) > 0:
#             publish_index = select_record_index(publisher_records)
#             return publisher_records.data[publish_index].get('rclcpp_intra_publish_timestamp')

#         return None

#     def _is_consistent_inter(
#         self,
#         publish_time: int,
#         callback_info: Union[TimerCallbackValueLttng,
#                              SubscriptionCallbackValueLttng]
#     ) -> Optional[bool]:
#         callback_object = callback_info.callback_object
#         cb_records = self._source.callback_records.clone()
#         cb_records.filter_if(lambda x: x.get(
#             'callback_object') == callback_object)

#         for data in cb_records.data:
#             if 'callback_start_timestamp' not in data.columns:
#                 continue
#             if 'callback_end_timestamp' not in data.columns:
#                 continue
#             if data.get('callback_start_timestamp') < publish_time and \
#                     publish_time < data.get('callback_end_timestamp'):
#                 return True
#             if data.get('callback_start_timestamp') > publish_time and \
#                     data.get('callback_end_timestamp') > publish_time:
#                 return False
#         return None

#     def _is_consistent_intra(
#         self,
#         publish_time: int,
#         callback: SubscriptionCallbackValueLttng
#     ) -> bool:
#         callback_object = callback.callback_object_intra
#         cb_records = self._source.callback_records.clone()
#         cb_records.filter_if(lambda x: x.get(
#             'callback_object') == callback_object)

#         for data in cb_records.data:
#             if 'callback_start_timestamp' not in data.columns:
#                 continue
#             if 'callback_end_timestamp' not in data.columns:
#                 continue
#             if data.get('callback_start_timestamp') < publish_time and \
#                     publish_time < data.get('callback_end_timestamp'):
#                 return True
#             if data.get('callback_start_timestamp') > publish_time and \
#                     data.get('callback_end_timestamp') > publish_time:
#                 return False
#         return False

#     def _is_consistent(
#         self,
#         publisher_info: PublisherValueLttng,
#         callback_info: Union[TimerCallbackValueLttng,
#                              SubscriptionCallbackValueLttng]
#     ) -> bool:
#         publish_time = self._get_publish_time(publisher_info)
#         if publish_time is None:
#             return False

#         is_consistent = self._is_consistent_inter(publish_time, callback_info)
#         if is_consistent is True:
#             return True

#         if isinstance(callback_info, SubscriptionCallbackValueLttng):
#             return self._is_consistent_intra(publish_time, callback_info)
#         return False

#     @staticmethod
#     def _update_timer_cb_publish_topics(
#         timer_cbs: List[TimerCallbackValueLttng],
#         update_target: TimerCallbackValueLttng,
#         publish_topic_names: Tuple[str, ...]
#     ) -> None:
#         # try:
#         index = timer_cbs.index(update_target)
#         timer_cbs[index] = TimerCallbackValueLttng(
#             callback_id=update_target.callback_id,
#             node_id=update_target.node_id,
#             node_name=update_target.node_name,
#             symbol=update_target.symbol,
#             period_ns=update_target.period_ns,
#             publish_topic_names=publish_topic_names,
#             callback_object=update_target.callback_object
#         )
#         # except ValueError:
#         #     print(f'Failed to find item. {update_target}.')

#     @staticmethod
#     def _update_sub_cb_publish_topics(
#         sub_cbs: List[SubscriptionCallbackValueLttng],
#         update_target: SubscriptionCallbackValueLttng,
#         publish_topic_names: Tuple[str, ...]
#     ) -> None:
#         index = sub_cbs.index(update_target)
#         sub_cbs[index] = SubscriptionCallbackValueLttng(
#             callback_id=update_target.callback_id,
#             node_id=update_target.node_id,
#             node_name=update_target.node_name,
#             symbol=update_target.symbol,
#             subscribe_topic_name=update_target.subscribe_topic_name,
#             publish_topic_names=publish_topic_names,
#             callback_object=update_target.callback_object,
#             callback_object_intra=update_target.callback_object_intra
#         )


class DataFrameFormatted:

    def __init__(self, data: Ros2DataModel):
        self._executor_df = self._build_executor_df(data)
        self._nodes_df = self._build_nodes_df(data)
        self._timer_callbacks_df = self._build_timer_callbacks_df(data)
        self._sub_callbacks_df = self._build_sub_callbacks_df(data)
        self._srv_callbacks_df = self._build_srv_callbacks_df(data)
        self._cbg_df = self._build_cbg_df(data)
        self._pub_df = self._build_publisher_df(data)
        self._tilde_sub = self._build_tilde_subscription_df(data)
        self._tilde_pub = self._build_tilde_publisher_df(data)
        self._tilde_sub_id_to_sub = self._build_tilde_sub_id_df(data, self._tilde_sub)
        self._timer_control = self._build_timer_control_df(data)

    @staticmethod
    def _ensure_columns(
        df: pd.DataFrame,
        columns: List[str],
    ) -> pd.DataFrame:
        df_ = df.copy()
        for missing_column in set(columns) - set(df.columns):
            df_[missing_column] = np.nan
        return df_

    @cached_property
    def tilde_sub_id_map(self) -> Dict[int, int]:
        d: Dict[int, int] = {}
        for _, row in self._tilde_sub_id_to_sub.iterrows():
            d[row['subscription_id']] = row['tilde_subscription']
        return d

    @property
    def timer_callbacks_df(self) -> pd.DataFrame:
        """
        Build timer callbacks table.

        Parameters
        ----------
        data : Ros2DataModel

        Returns
        -------
        pd.DataFrame
            Column
            - callback_object
            - node_handle
            - timer_handle
            - callback_group_addr
            - period_ns,
            - symbol
            - callback_id

        """
        return self._timer_callbacks_df

    @property
    def subscription_callbacks_df(self) -> pd.DataFrame:
        """
        Build subscription callback table.

        Parameters
        ----------
        data : Ros2DataModel

        Returns
        -------
        pd.DataFrame
            columns
            - callback_object
            - callback_object_intra
            - node_handle
            - subscription_handle
            - callback_group_addr
            - topic_name
            - symbol
            - callback_id
            - depth

        """
        return self._sub_callbacks_df

    @property
    def nodes_df(self) -> pd.DataFrame:
        """
        Build node table.

        Parameters
        ----------
        data : Ros2DataModel

        Returns
        -------
        pd.DataFrame
            Columns
            - node_handle
            - node_name

        """
        return self._nodes_df

    @property
    def publishers_df(self) -> pd.DataFrame:
        """
        Get publisher info table.

        Returns
        -------
        pd.DataFrame
            Columns
            - publisher_handle
            - node_handle
            - topic_name
            - depth

        """
        return self._pub_df

    @property
    def services_df(self) -> pd.DataFrame:
        """
        Get service info table.

        Returns
        -------
        pd.DataFrame
            Columns
            - callback_id
            - callback_object
            - node_handle
            - service_handle
            - service_name
            - symbol

        """
        return self._srv_callbacks_df

    @property
    def executor_df(self) -> pd.DataFrame:
        """
        Get executor info table.

        Returns
        -------
        pd.DataFrame
            Columns
            - executor_addr
            - executor_type_name

        """
        return self._executor_df

    @property
    def callback_groups_df(self) -> pd.DataFrame:
        """
        Get callback group info table.

        Returns
        -------
        pd.DataFrame
            Columns
            - callback_group_addr
            - executor_addr
            - group_type_name

        """
        return self._cbg_df

    @property
    def tilde_publishers_df(self) -> pd.DataFrame:
        """
        Get tilde wrapped publisher.

        Returns
        -------
        pd.DataFrame
            Columns
            - tilde_publisher
            - node_name
            - topic_name

        """
        return self._tilde_pub

    @property
    def tilde_subscriptions_df(self) -> pd.DataFrame:
        """
        Get tilde wrapped subscription.

        Returns
        -------
        pd.DataFrame
            Columns
            - tilde_subscription
            - node_name
            - topic_name

        """
        return self._tilde_sub

    @property
    def timer_controls_df(self) -> pd.DataFrame:
        return self._timer_control

    @staticmethod
    def _build_publisher_df(
        data: Ros2DataModel,
    ) -> pd.DataFrame:
        columns = ['publisher_id', 'publisher_handle', 'node_handle', 'topic_name', 'depth']
        df = data.publishers.reset_index()

        def to_publisher_id(row: pd.Series):
            publisher_handle = row['publisher_handle']
            return f'publisher_{publisher_handle}'

        df = DataFrameFormatted._add_column(df, 'publisher_id', to_publisher_id)
        df = DataFrameFormatted._ensure_columns(df, columns)
        return df[columns]

    @staticmethod
    def _build_timer_control_df(
        data: Ros2DataModel,
    ) -> pd.DataFrame:
        columns = ['timestamp', 'timer_handle', 'type', 'params']
        df = data.timers.reset_index()
        df['type'] = 'init'
        df['params'] = [{'period': row['period']} for (_, row) in df.iterrows()]
        df = DataFrameFormatted._ensure_columns(df, columns)
        return df[columns]

    @staticmethod
    def _build_executor_df(
        data: Ros2DataModel,
    ) -> pd.DataFrame:
        columns = ['executor_id', 'executor_addr', 'executor_type_name']

        df = data.executors.reset_index()

        df_ = data.executors_static.reset_index()
        if len(df_) > 0:
            columns_ = columns[1:]  # ignore executor_id
            df = concat(columns_, [df, df_])

        def to_executor_id(row: pd.Series) -> str:
            addr = row['executor_addr']
            return f'executor_{addr}'

        df = DataFrameFormatted._add_column(df, 'executor_id', to_executor_id)
        df = DataFrameFormatted._ensure_columns(df, columns)

        df = df[columns]

        # data.callback_groups returns duplicate results that differ only in timestamp.
        # Remove duplicates to make it unique.
        df.drop_duplicates(inplace=True)

        return df[columns]

    @staticmethod
    def _build_cbg_df(
        data: Ros2DataModel,
    ) -> pd.DataFrame:
        columns = ['callback_group_id', 'callback_group_addr', 'group_type_name', 'executor_addr']
        df = data.callback_groups.reset_index()

        df_static = data.callback_groups_static.reset_index()
        df_static_exec = data.executors_static.reset_index()

        if len(df_static) > 0 and len(df_static_exec) > 0:
            df_static = merge(df_static, df_static_exec, 'entities_collector_addr')
            columns_ = columns[1:]  # ignore callback_group_id
            df = concat(columns_, [df, df_static])

        def to_callback_group_id(row: pd.Series) -> str:
            addr = row['callback_group_addr']
            return f'callback_group_{addr}'

        df = DataFrameFormatted._add_column(df, 'callback_group_id', to_callback_group_id)
        df = DataFrameFormatted._ensure_columns(df, columns)
        df = df[columns]

        # data.callback_groups returns duplicate results that differ only in timestamp.
        # Remove duplicates to make it unique.
        df.drop_duplicates(inplace=True)

        executor_duplicated_indexes = []
        for _, group in df.groupby('callback_group_addr'):
            if len(group) >= 2:
                msg = ('Multiple executors using the same callback group were detected.'
                       'The last executor will be used. ')
                exec_addr = list(group['executor_addr'].values)
                msg += f'executor address: {exec_addr}'
                logger.warn(msg)
                executor_duplicated_indexes += list(group.index)[:-1]

        if len(executor_duplicated_indexes) >= 1:
            df.drop(index=executor_duplicated_indexes, inplace=True)

        df.reset_index(drop=True, inplace=True)
        return df

    @staticmethod
    def _build_timer_callbacks_df(
        data: Ros2DataModel,
    ) -> pd.DataFrame:
        columns = [
            'callback_id', 'callback_object', 'node_handle', 'timer_handle', 'callback_group_addr',
            'period_ns', 'symbol',
        ]

        def callback_id(row: pd.Series) -> str:
            cb_object = row['callback_object']
            return f'timer_callback_{cb_object}'
        try:
            df = data.timers.reset_index()

            timer_node_links_df = data.timer_node_links.reset_index()
            df = merge(df, timer_node_links_df, 'timer_handle')

            callback_objects_df = data.callback_objects.reset_index().rename(
                columns={'reference': 'timer_handle'})
            df = merge(df, callback_objects_df, 'timer_handle')

            symbols_df = data.callback_symbols
            df = merge(df, symbols_df, 'callback_object')

            cbg = data.callback_group_timer.reset_index()
            df = merge(df, cbg, 'timer_handle')

            df = DataFrameFormatted._add_column(df, 'callback_id', callback_id)

            df.rename({'period': 'period_ns'}, inplace=True, axis=1)

            df = DataFrameFormatted._ensure_columns(df, columns)

            return df[columns]
        except KeyError:
            return pd.DataFrame(columns=columns)

    @staticmethod
    def _build_sub_callbacks_df(
        data: Ros2DataModel,
    ) -> pd.DataFrame:
        columns = [
            'callback_id', 'callback_object', 'callback_object_intra', 'node_handle',
            'subscription_handle', 'callback_group_addr', 'topic_name', 'symbol', 'depth'
        ]

        def callback_id(row: pd.Series) -> str:
            cb_object = row['callback_object']
            return f'subscription_callback_{cb_object}'

        try:
            df = data.subscriptions.reset_index()

            callback_objects_df = DataFrameFormatted._format_subscription_callback_object(data)
            df = merge(df, callback_objects_df, 'subscription_handle')

            symbols_df = data.callback_symbols.reset_index()
            df = merge(df, symbols_df, 'callback_object')

            cbg = data.callback_group_subscription.reset_index()
            df = merge(df, cbg, 'subscription_handle')

            df = DataFrameFormatted._add_column(
                df, 'callback_id', callback_id
            )

            df = DataFrameFormatted._ensure_columns(df, columns)

            return df[columns].convert_dtypes()
        except KeyError:
            return pd.DataFrame(columns=columns).convert_dtypes()

    @staticmethod
    def _build_srv_callbacks_df(
        data: Ros2DataModel,
    ) -> pd.DataFrame:
        columns = [
            'callback_id', 'callback_object', 'node_handle',
            'service_handle', 'service_name', 'symbol'
        ]

        def callback_id(row: pd.Series) -> str:
            cb_object = row['callback_object']
            return f'service_callback_{cb_object}'

        try:
            df = data.services.reset_index()

            callback_objects_df = data.callback_objects.reset_index().rename(
                {'reference': 'service_handle'}, axis=1)
            df = merge(df, callback_objects_df, 'service_handle')

            symbols_df = data.callback_symbols.reset_index()
            df = merge(df, symbols_df, 'callback_object')

            df = DataFrameFormatted._add_column(
                df, 'callback_id', callback_id
            )

            df = DataFrameFormatted._ensure_columns(df, columns)

            return df[columns]
        except KeyError:
            return pd.DataFrame(columns=columns)

    @staticmethod
    def _build_tilde_subscription_df(
        data: Ros2DataModel,
    ) -> pd.DataFrame:
        columns = ['tilde_subscription', 'node_name', 'topic_name']

        try:
            df = data.tilde_subscriptions.reset_index()

            df.rename({'subscription': 'tilde_subscription'}, axis=1, inplace=True)
            df = DataFrameFormatted._ensure_columns(df, columns)

            return df[columns]
        except KeyError:
            return pd.DataFrame(columns=columns)

    @staticmethod
    def _build_tilde_publisher_df(
        data: Ros2DataModel,
    ) -> pd.DataFrame:
        columns = ['tilde_publisher', 'tilde_subscription', 'node_name', 'topic_name']

        try:
            df = data.tilde_publishers.reset_index()

            df.rename({'publisher': 'tilde_publisher'}, axis=1, inplace=True)
            df = DataFrameFormatted._ensure_columns(df, columns)

            return df[columns]
        except KeyError:
            return pd.DataFrame(columns=columns)

    @staticmethod
    def _build_tilde_sub_id_df(
        data: Ros2DataModel,
        sub_df: pd.DataFrame
    ) -> pd.DataFrame:
        columns = ['subscription_id', 'tilde_subscription', 'node_name', 'topic_name']
        try:
            df = data.tilde_subscribe_added.reset_index()
            df = pd.merge(df, sub_df, on=['node_name', 'topic_name'], how='left')

            df = DataFrameFormatted._ensure_columns(df, columns)

            return df[columns]
        except KeyError:
            return pd.DataFrame(columns=columns)

    @staticmethod
    def _build_tilde_sub(
        data: Ros2DataModel,
    ) -> pd.DataFrame:
        columns = [
            'callback_id', 'callback_object', 'callback_object_intra', 'node_handle',
            'subscription_handle', 'callback_group_addr', 'topic_name', 'symbol', 'depth'
        ]

        def callback_id(row: pd.Series) -> str:
            cb_object = row['callback_object']
            return f'subscription_callback_{cb_object}'

        try:
            df = data.subscriptions.reset_index()

            callback_objects_df = DataFrameFormatted._format_subscription_callback_object(data)
            df = merge(df, callback_objects_df, 'subscription_handle')

            symbols_df = data.callback_symbols.reset_index()
            df = merge(df, symbols_df, 'callback_object')

            cbg = data.callback_group_subscription.reset_index()
            df = merge(df, cbg, 'subscription_handle')

            df = DataFrameFormatted._add_column(
                df, 'callback_id', callback_id
            )

            df = DataFrameFormatted._ensure_columns(df, columns)

            return df[columns]
        except KeyError:
            return pd.DataFrame(columns=columns)

    @staticmethod
    def _is_ignored_subscription(
        data: Ros2DataModel,
        subscription_handle: int
    ) -> bool:
        sub_df = data.subscriptions
        sub_df = sub_df[sub_df.index == subscription_handle]
        sub_df = merge(sub_df, data.nodes, 'node_handle')

        ns = sub_df.at[0, 'namespace']
        name = sub_df.at[0, 'name']
        topic_name = sub_df.at[0, 'topic_name']

        if ns == '/' and name == 'rviz2':
            return True

        if topic_name == '/parameter_events':
            return True

        return False

    @staticmethod
    def _format_subscription_callback_object(
        data: Ros2DataModel,
    ) -> pd.DataFrame:
        """
        Split the callback_object of the subscription callback.

        into callback_object and callback_object_intra.

        Parameters
        ----------
        data : Ros2DataModel

        Returns
        -------
        pd.DataFrame
            columns
            - subscription_handle
            - callback_object
            - callback_object_intra [Optional]

        Raises
        ------
        InvalidArgumentError

        """
        callback_objects_df = data.callback_objects.reset_index().rename(
            columns={'reference': 'subscription'})
        sub_df = data.subscription_objects
        sub_df = merge(sub_df, callback_objects_df, 'subscription')

        dicts = []
        for key, group in sub_df.groupby('subscription_handle'):
            group.reset_index(drop=True, inplace=True)

            if DataFrameFormatted._is_ignored_subscription(data, key):
                continue

            record = {
                'subscription_handle': key,
            }
            if len(group) == 1:
                record['callback_object'] = group.at[0, 'callback_object']
            elif len(group) == 2:
                record['callback_object'] = group.at[1, 'callback_object']
                record['callback_object_intra'] = group.at[0, 'callback_object']
            else:
                cb_objs = group['callback_object'].values
                logger.warning(
                    'More than three callbacks are registered in one subscription_handle. '
                    'Skip loading callback info. The following callbacks cannot be measured.'
                    f'subscription_handle = {key}, '
                    f'callback_objects = {cb_objs}')
            dicts.append(record)
        df = pd.DataFrame.from_dict(dicts, dtype='Int64')
        return df

    @staticmethod
    def _add_column(
        df: pd.DataFrame,
        column_name: str,
        cell_rule: Callable[[pd.Series], str]
    ) -> pd.DataFrame:
        from copy import deepcopy
        df_ = deepcopy(df)

        df_[column_name] = ''
        for i in range(len(df_)):
            row = df_.iloc[i, :]
            df_.loc[i, column_name] = cell_rule(row)

        return df_

    @staticmethod
    def _build_nodes_df(
        data: Ros2DataModel
    ) -> pd.DataFrame:
        columns = ['node_id', 'node_handle', 'node_name']

        node_df = data.nodes.reset_index()

        def ns_and_node_name(row: pd.Series) -> str:
            ns: str = row['namespace']
            name: str = row['name']

            if ns[-1] == '/':
                return ns + name
            else:
                return ns + '/' + name

        node_df = DataFrameFormatted._add_column(node_df, 'node_name', ns_and_node_name)

        def to_node_id(row: pd.Series) -> str:
            node_name = row['node_name']
            node_handle = row['node_handle']
            return f'{node_name}_{node_handle}'

        node_df = DataFrameFormatted._add_column(node_df, 'node_id', to_node_id)

        node_df.drop(['namespace', 'name'], inplace=True, axis=1)

        node_df.reset_index(drop=True, inplace=True)
        return node_df[columns]


def merge(
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    join_key: str,
    drop_columns: List[str] = ['timestamp', 'tid', 'rmw_handle']
) -> pd.DataFrame:

    def drop(df: pd.DataFrame, drop_columns: List[str]):
        columns = set(df) & set(drop_columns)
        if len(columns) == 0:
            return df
        return df.drop(columns, axis=1)

    left_df_ = drop(left_df, drop_columns)
    right_df_ = drop(right_df, drop_columns)
    return pd.merge(
        left_df_,
        right_df_,
        left_on=join_key,
        right_on=join_key
    )


def concat(
    column_names: List[str],
    dfs: List[pd.DataFrame],
) -> pd.DataFrame:
    concat_targets = []
    for df in dfs:
        has_columns = (set(df.columns) & set(
            column_names)) == set(column_names)
        if not has_columns:
            continue
        concat_targets.append(df[column_names])

    return pd.concat(concat_targets, axis=0).reset_index(drop=True)
