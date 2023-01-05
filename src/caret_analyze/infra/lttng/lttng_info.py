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

from collections import defaultdict
from functools import cached_property, lru_cache
from logging import getLogger, WARN
from typing import Dict, List, Optional, Sequence, Tuple, Union

from caret_analyze.infra.lttng.value_objects.timer_control import TimerInit
from caret_analyze.value_objects.timer import TimerValue

import pandas as pd

from .ros2_tracing.data_model import Ros2DataModel
from .ros2_tracing.data_model_service import DataModelService
from .value_objects import (CallbackGroupAddr,
                            CallbackGroupValueLttng,
                            NodeValueLttng,
                            PublisherValueLttng,
                            ServiceCallbackValueLttng,
                            SubscriptionCallbackValueLttng,
                            TimerCallbackValueLttng,
                            TimerControl)
from ..trace_point_data import TracePointData, TracePointIntermediateData
from ...common import Util
from ...exceptions import InvalidArgumentError
from ...value_objects import ExecutorValue, NodeValue, Qos


logger = getLogger(__name__)


class LttngInfo:

    def __init__(self, data: Ros2DataModel):
        self._formatted = DataFrameFormatted(data)

        # TODO(hsgwa): check rmw_impl for each process.
        self._rmw_implementation = data.rmw_impl.iat(0, 0) if len(data.rmw_impl) > 0 else ''
        # self._source = records_source
        # self._binder_cache: Dict[str, PublisherBinder] = {}

        self._timer_cb_cache: Dict[str, Sequence[TimerCallbackValueLttng]] = {}
        self._sub_cb_cache: Dict[str, List[SubscriptionCallbackValueLttng]] = {}
        self._srv_cb_cache: Dict[str, List[ServiceCallbackValueLttng]] = {}
        self._pub_cache: Dict[str, List[PublisherValueLttng]] = {}
        self._cbg_cache: Dict[str, List[CallbackGroupValueLttng]] = {}

        self._id_to_topic: Dict[str, str] = {}
        self._id_to_service: Dict[str, str] = {}
        self._sub_cb_cache_without_pub: Optional[Dict[str, List[SubscriptionCallbackValueLttng]]]
        self._sub_cb_cache_without_pub = None

        self._srv_cb_cache_without_pub: Optional[Dict[str, List[ServiceCallbackValueLttng]]]
        self._srv_cb_cache_without_pub = None

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

    def _get_srv_cbs_without_pub(self, node_id: str) -> List[ServiceCallbackValueLttng]:
        if self._srv_cb_cache_without_pub is None:
            self._srv_cb_cache_without_pub = self._load_srv_cbs_without_pub()

        if node_id not in self._srv_cb_cache_without_pub:
            return []

        return self._srv_cb_cache_without_pub[node_id]

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

        timer_callbacks = self._formatted.timer_callbacks.clone()
        nodes = self._formatted.nodes.clone()
        merge(timer_callbacks, nodes, 'node_handle')

        for _, row in timer_callbacks.df.iterrows():
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
                    callback_object=row['callback_object'],
                    construction_order=row['construction_order'],
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
        node : NodeValue
            target node.

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
        nodes_data = self._formatted.nodes.clone()

        nodes = []
        added_nodes = set()
        duplicate_nodes = set()

        for _, row in nodes_data.df.iterrows():
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

        sub = self._formatted.subscription_callbacks.clone()
        nodes = self._formatted.nodes.clone()
        merge(sub, nodes, 'node_handle')
        tilde_sub = self._formatted.tilde_subscriptions.clone()
        sub.merge(tilde_sub, ['node_name', 'topic_name'], how='left')

        for _, row in sub.df.iterrows():
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
                    tilde_subscription=tilde_subscription,
                    construction_order=row['construction_order']
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
        Get subscription callbacks information.

        Parameters
        ----------
        node : NodeValue
            target node.

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

    def _load_srv_cbs_without_pub(
        self
    ) -> Dict[str, List[ServiceCallbackValueLttng]]:
        srv_cbs_info: Dict[str, List[ServiceCallbackValueLttng]] = {}

        for node in self.get_nodes():
            srv_cbs_info[node.node_id] = []

        srv = self._formatted.service_callbacks.clone()
        nodes = self._formatted.nodes.clone()
        merge(srv, nodes, 'node_handle')

        for _, row in srv.df.iterrows():
            node_name = row['node_name']
            node_id = row['node_id']

            self._id_to_service[row['callback_id']] = row['service_name']

            srv_cbs_info[node_id].append(
                ServiceCallbackValueLttng(
                    callback_id=row['callback_id'],
                    node_id=node_id,
                    node_name=node_name,
                    symbol=row['symbol'],
                    service_name=row['service_name'],
                    service_handle=row['service_handle'],
                    publish_topic_names=None,
                    callback_object=row['callback_object'],
                    construction_order=row['construction_order']
                )
            )
        return srv_cbs_info

    def _get_service_callback_values(
        self,
        node: NodeValue
    ) -> List[ServiceCallbackValueLttng]:
        node_id = node.node_id
        assert node_id is not None

        srv_cbs_info: List[ServiceCallbackValueLttng]
        srv_cbs_info = self._get_srv_cbs_without_pub(node_id)

        return srv_cbs_info

    def get_service_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[ServiceCallbackValueLttng]:
        """
        Get service callbacks information.

        Parameters
        ----------
        node : NodeValue
            target node.

        Returns
        -------
        Sequence[ServiceCallbackInfo]

        """
        def get_srv_cb_local(node: NodeValueLttng):
            node_id = node.node_id
            if node_id not in self._srv_cb_cache.keys():
                self._srv_cb_cache[node_id] = self._get_service_callback_values(node)
            return self._srv_cb_cache[node_id]

        if node.node_id is None:
            return Util.flatten([
                get_srv_cb_local(node)
                for node
                in self._get_nodes(node.node_name)
            ])

        node_lttng = NodeValueLttng(node.node_name, node.node_id)
        return get_srv_cb_local(node_lttng)

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
        node_id : str
            node ID

        Returns
        -------
        List[PublisherInfo]

        """
        pub = self._formatted.publishers.clone()
        nodes = self._formatted.nodes.clone()
        merge(pub, nodes, 'node_handle')
        tilde_pub = self._formatted.tilde_publishers.clone()

        pub.merge(tilde_pub, ['node_name', 'topic_name'], how='left')
        # pub = pub.astype({'tilde_publisher': 'Int64'})
        pubs_info = []
        for _, row in pub.df.iterrows():
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
        is_service = callback_id in self._id_to_service.keys()
        if not is_service:
            return True
        topic_name = self._id_to_topic[callback_id]

        return topic_name not in ['/clock', '/parameter_events']

    def _get_callback_groups(
        self,
        node_id: str
    ) -> List[CallbackGroupValueLttng]:
        concat_target_dfs = []
        concat_target_dfs.append(self._formatted.timer_callbacks.clone())
        concat_target_dfs.append(self._formatted.subscription_callbacks.clone())
        concat_target_dfs.append(self._formatted.service_callbacks.clone())

        try:
            column_names = [
                'callback_group_addr', 'callback_id', 'node_handle'
            ]
            concat = TracePointData.concat(concat_target_dfs, column_names)

            nodes = self._formatted.nodes.clone()
            callback_groups = self._formatted.callback_groups.clone()
            merge(concat, nodes, 'node_handle')
            merge(concat, callback_groups, 'callback_group_addr')

            callback_groups_values: List[CallbackGroupValueLttng] = []
            for _, group_df in concat.df.groupby('callback_group_addr'):
                row = group_df.iloc[0, :]
                node_id_ = row['node_id']
                if node_id != node_id_:
                    continue

                callback_ids = tuple(group_df['callback_id'].values)
                callback_ids = tuple(Util.filter_items(self._is_user_made_callback, callback_ids))

                callback_groups_values.append(
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

            return callback_groups_values
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
        executor = self._formatted.executor.clone()
        callback_groups = self._formatted.callback_groups.clone()
        merge(executor, callback_groups, 'executor_addr')
        execs = []

        for _, group in executor.df.groupby('executor_addr'):
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
        publishers = self._formatted.publishers.clone()
        publishers.filter_rows('publisher_handle', publisher.publisher_handle)

        if len(publishers) == 0:
            raise InvalidArgumentError('No publisher matching the criteria was found.')
        if len(publishers) > 1:
            logger.warning(
                'Multiple publishers matching your criteria were found.'
                'The value of the first publisher qos will be returned.')

        depth = int(publishers.at('depth', 0))
        return Qos(depth)

    def get_subscription_qos(self, callback: SubscriptionCallbackValueLttng) -> Qos:
        subscription = self._formatted.subscription_callbacks.clone()
        subscription.filter_rows('callback_object', callback.callback_object)

        if len(subscription) == 0:
            raise InvalidArgumentError('No subscription matching the criteria was found.')
        if len(subscription) > 1:
            logger.warning(
                'Multiple publishers matching your criteria were found.'
                'The value of the first publisher qos will be returned.')

        depth = int(subscription.at('depth', 0))
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
        timer_controls = self._formatted.timer_controls.clone()
        controls: List[TimerControl] = []
        for _, row in timer_controls.df.iterrows():
            if row['type'] == 'init':
                params = row['params']
                control = TimerInit(
                    int(row['timer_handle']),
                    int(row['timestamp']),
                    int(params['period']))
                controls.append(control)
            else:
                raise NotImplementedError('Unsupported timer control type.')

        return controls


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

#         callback_groups: Sequence[CallbackGroupValueLttng]
#         callback_groups = self._info.get_callback_groups(node)

#         # TODO: ignore /parameter_events, /clock
#         # Ignore callback groups that have no callbacks added,
#         # as they are irrelevant to performance.
#         # if len(callback_ids) == 0:
#         #     self._ignored_callback_groups.add(row['callback_group_id'])
#         #     continue

#         if len(callback_groups) != 1:
#             print('false')
#             return False

#         cbg = callback_groups[0]
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
#         Return publisher bound callback values.

#         Note:
#         This function call takes a long time because binding uses records.

#         Parameters
#         ----------
#         node_name : str
#         callbacks_info : Sequence[TimerCallbackValueLttng]

#         Returns
#         -------
#         List[TimerCallbackValueLttng]
#             publisher bound callback values.

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
#         Return publisher bound callback values.

#         Note:
#         This function call takes a long time because binding uses records.

#         Parameters
#         ----------
#         node_name : str
#         callbacks_info : Sequence[SubscriptionCallbackValueLttng]

#         Returns
#         -------
#         List[SubscriptionCallbackValueLttng]
#             publisher bound callback values.

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
#             # Note: intra_process cyclic demo is manually publishing the first message.
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
        self._executor_df = self._build_executor(data)
        self._nodes = self._build_nodes(data)
        self._timer_callbacks = self._build_timer_callbacks(data)
        self._sub_callbacks = self._build_sub_callbacks(data)
        self._srv_callbacks = self._build_srv_callbacks(data)
        self._cbg = self._build_cbg(data)
        self._pub = self._build_publisher(data)
        self._tilde_sub = self._build_tilde_subscription(data)
        self._tilde = self._build_tilde_publisher(data)
        self._tilde_sub_id_to_sub = self._build_tilde_sub_id(data, self._tilde_sub)
        self._timer_control = self._build_timer_control(data)

    @cached_property
    def tilde_sub_id_map(self) -> Dict[int, int]:
        d: Dict[int, int] = {}
        for _, row in self._tilde_sub_id_to_sub.df.iterrows():
            d[row['subscription_id']] = row['tilde_subscription']
        return d

    @property
    def timer_callbacks(self) -> TracePointData:
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
        return self._timer_callbacks

    @property
    def subscription_callbacks(self) -> TracePointData:
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
        return self._sub_callbacks

    @property
    def service_callbacks(self) -> TracePointData:
        """
        Build service callback table.

        Parameters
        ----------
        data : Ros2DataModel

        Returns
        -------
        pd.DataFrame
            Columns
            - callback_id
            - callback_object
            - callback_group_addr
            - node_handle
            - service_handle
            - service_name
            - symbol

        """
        return self._srv_callbacks

    @property
    def nodes(self) -> TracePointData:
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
        return self._nodes

    @property
    def publishers(self) -> TracePointData:
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
        return self._pub

    @property
    def executor(self) -> TracePointData:
        """
        Get executor info table.

        Returns
        -------
        pd.DataFrame
            Columns
            - executor_addr
            - executor_type_name

        """
        return self._executor_df.clone()

    @property
    def callback_groups(self) -> TracePointData:
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
        return self._cbg

    @property
    def tilde_publishers(self) -> TracePointData:
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
        return self._tilde

    @property
    def tilde_subscriptions(self) -> TracePointData:
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
    def timer_controls(self) -> TracePointData:
        return self._timer_control

    @staticmethod
    def _build_publisher(
        data: Ros2DataModel,
    ) -> TracePointData:
        columns = ['publisher_id', 'publisher_handle', 'node_handle', 'topic_name', 'depth']
        publishers = data.publishers.clone()
        publishers.reset_index()

        def to_publisher_id(row: pd.Series):
            publisher_handle = row['publisher_handle']
            return f'publisher_{publisher_handle}'

        publishers.add_column('publisher_id', to_publisher_id)
        publishers.set_columns(columns)
        return publishers

    @staticmethod
    def _build_timer_control(
        data: Ros2DataModel,
    ) -> TracePointData:
        columns = ['timestamp', 'timer_handle', 'type', 'params']
        timers = data.timers.clone()
        timers.reset_index()
        timers.add_column('type', lambda _: 'init')

        def to_params(row: pd.Series):
            return {'period': row['period']}

        timers.add_column('params', to_params)
        timers.set_columns(columns)
        return timers

    @staticmethod
    def _build_executor(
        data: Ros2DataModel,
    ) -> TracePointData:
        columns = ['executor_id', 'executor_addr', 'executor_type_name']

        executors = data.executors.clone()
        executors.reset_index()
        executors_static = data.executors_static.clone()
        executors_static.reset_index()

        if len(executors_static) > 0:
            columns_ = columns[1:]  # ignore executor_id
            executors = TracePointData.concat(
                [executors, executors_static], columns_)

        def to_executor_id(row: pd.Series) -> str:
            addr = row['executor_addr']
            return f'executor_{addr}'

        executors.add_column('executor_id', to_executor_id)
        executors.set_columns(columns)

        # data.callback_groups returns duplicate results that differ only in timestamp.
        # Remove duplicates to make it unique.
        executors.drop_duplicate()

        return executors

    @staticmethod
    def _build_cbg(
        data: Ros2DataModel,
    ) -> TracePointData:
        columns = ['callback_group_id', 'callback_group_addr', 'group_type_name', 'executor_addr']
        callback_groups = data.callback_groups.clone()
        callback_groups.reset_index()

        callback_groups_static = data.callback_groups_static.clone()
        callback_groups_static.reset_index()

        executors_static = data.executors_static.clone()
        executors_static.reset_index()

        if len(callback_groups_static) > 0 and len(executors_static) > 0:
            merge(callback_groups_static, executors_static, 'entities_collector_addr')
            columns_ = columns[1:]  # ignore callback_group_id
            callback_groups = TracePointData.concat(
                [callback_groups, callback_groups_static], columns_)

        def to_callback_group_id(row: pd.Series) -> str:
            addr = row['callback_group_addr']
            return CallbackGroupAddr(addr).group_id

        callback_groups.add_column('callback_group_id', to_callback_group_id)
        callback_groups.set_columns(columns)

        # data.callback_groups returns duplicate results that differ only in timestamp.
        # Remove duplicates to make it unique.
        callback_groups.drop_duplicate()

        executor_duplicated_indexes = []
        for _, group in callback_groups.df.groupby('callback_group_addr'):
            if len(group) >= 2:
                msg = ('Multiple executors using the same callback group were detected. '
                       'The last executor will be used. ')
                exec_addr = list(group['executor_addr'].values)
                msg += f'executor address: {exec_addr}. '
                data_model_srv = DataModelService(data)
                cbg_addr = set(group['callback_group_addr'].values)
                for addr in cbg_addr:
                    node_names_and_cb_symbols = data_model_srv.get_node_names_and_cb_symbols(addr)
                    msg += f'callback_group_addr: {addr}.\n'
                    for i, node_name_and_cb_symbol in enumerate(node_names_and_cb_symbols):
                        msg += f'\t|node name {i}| {node_name_and_cb_symbol[0]}.\n'
                        msg += f'\t|callback symbol {i}| {node_name_and_cb_symbol[1]}.\n'
                # This warning occurs frequently,
                # but currently does not significantly affect behavior.
                # Therefore, the log level is temporarily lowered.
                logger.log(WARN-1, msg)
                executor_duplicated_indexes += list(group.index)[:-1]

        if len(executor_duplicated_indexes) >= 1:
            callback_groups.drop_row(executor_duplicated_indexes)

        return callback_groups

    @staticmethod
    def _build_timer_callbacks(
        data: Ros2DataModel,
    ) -> TracePointData:
        columns = [
            'callback_id', 'callback_object', 'node_handle', 'timer_handle', 'callback_group_addr',
            'period_ns', 'symbol', 'construction_order'
        ]

        def callback_id(row: pd.Series) -> str:
            cb_object = row['callback_object']
            return f'timer_callback_{cb_object}'
        timers = data.timers.clone()
        timers.reset_index()
        timers.rename_column('period', 'period_ns')

        merge_drop_columns = ['tid', 'rmw_handle']
        timer_node_links = data.timer_node_links.clone()
        timer_node_links.reset_index()
        timer_node_links.remove_column('timestamp')
        merge(timers, timer_node_links, 'timer_handle', merge_drop_columns=merge_drop_columns)

        callback_objects = data.callback_objects.clone()
        callback_objects.reset_index()
        callback_objects.rename_column('reference', 'timer_handle')
        callback_objects.remove_column('timestamp')
        merge(timers, callback_objects, 'timer_handle', merge_drop_columns=merge_drop_columns)

        symbols = data.callback_symbols.clone()
        symbols.reset_index()
        symbols.remove_column('timestamp')
        merge(timers, symbols, 'callback_object', merge_drop_columns=merge_drop_columns)

        DataFrameFormatted._add_construction_order(
            timers, 'construction_order', 'timestamp', 'node_handle', 'period_ns', 'symbol')

        callback_group_timer = data.callback_group_timer.clone()
        callback_group_timer.reset_index()
        merge(timers, callback_group_timer, 'timer_handle')

        timers.add_column('callback_id', callback_id)

        timers.set_columns(columns)

        return timers

    @staticmethod
    def _build_sub_callbacks(
        data: Ros2DataModel,
    ) -> TracePointData:
        columns = [
            'callback_id', 'callback_object', 'callback_object_intra', 'node_handle',
            'subscription_handle', 'callback_group_addr', 'topic_name', 'symbol', 'depth',
            'construction_order'
        ]

        def callback_id(row: pd.Series) -> str:
            cb_object = row['callback_object']
            return f'subscription_callback_{cb_object}'

        merge_drop_columns = ['tid', 'rmw_handle']

        subscriptions = data.subscriptions.clone()
        subscriptions.reset_index()

        subscription_callback_object = \
            DataFrameFormatted._format_subscription_callback_object(data)

        merge(subscriptions, subscription_callback_object, 'subscription_handle',
              merge_drop_columns=merge_drop_columns)

        symbols = data.callback_symbols.clone()
        symbols.reset_index()
        symbols.remove_column('timestamp')
        merge(subscriptions, symbols, 'callback_object', merge_drop_columns=merge_drop_columns)

        DataFrameFormatted._add_construction_order(
            subscriptions, 'construction_order', 'timestamp',
            'node_handle', 'topic_name', 'symbol')

        callback_group_subscription = data.callback_group_subscription.clone()
        callback_group_subscription.reset_index()
        merge(subscriptions, callback_group_subscription, 'subscription_handle')

        subscriptions.add_column('callback_id', callback_id)

        subscriptions.set_columns(columns)

        return subscriptions

    KeyType = Tuple[Union[int, str], Union[int, str], Union[str, int]]

    @staticmethod
    def _add_construction_order(
        data: TracePointData,
        column_name: str,
        timestamp_column: str,
        node_handle_column: str,
        callback_parameter_column: str,
        symbol_column: str
    ) -> None:

        data.sort(timestamp_column)
        order: defaultdict[DataFrameFormatted.KeyType, int] = defaultdict(int)

        def construct_order(row: pd.Series) -> int:
            node = row[node_handle_column]
            callback_param = row[callback_parameter_column]
            symbol = row[symbol_column]
            key: DataFrameFormatted.KeyType = node, callback_param, symbol
            order_ = int(order[key])
            order[key] += 1
            return order_

        data.add_column(column_name, construct_order)

    @staticmethod
    def _build_srv_callbacks(
        data: Ros2DataModel,
    ) -> TracePointData:
        columns = [
            'callback_id', 'callback_object', 'node_handle',
            'service_handle', 'callback_group_addr', 'service_name', 'symbol', 'construction_order'
        ]

        def callback_id(row: pd.Series) -> str:
            cb_object = row['callback_object']
            return f'service_callback_{cb_object}'

        merge_drop_columns = ['tid', 'rmw_handle']

        services = data.services.clone()
        services.reset_index()

        callback_objects = data.callback_objects.clone()
        callback_objects.reset_index()
        callback_objects.rename_column('reference', 'service_handle')
        callback_objects.remove_column('timestamp')
        merge(services, callback_objects, 'service_handle', merge_drop_columns=merge_drop_columns)

        symbols = data.callback_symbols.clone()
        symbols.reset_index()
        symbols.remove_column('timestamp')
        merge(services, symbols, 'callback_object', merge_drop_columns=merge_drop_columns)

        DataFrameFormatted._add_construction_order(
            services, 'construction_order',
            'timestamp', 'node_handle', 'service_name', 'symbol')

        callback_group_service = data.callback_group_service.clone()
        callback_group_service.reset_index()
        merge(services, callback_group_service, 'service_handle')

        services.add_column('callback_id', callback_id)

        services.set_columns(columns)

        return services

    @staticmethod
    def _build_tilde_subscription(
        data: Ros2DataModel,
    ) -> TracePointData:
        columns = ['tilde_subscription', 'node_name', 'topic_name']

        tilde_subscriptions = data.tilde_subscriptions.clone()
        tilde_subscriptions.reset_index()

        tilde_subscriptions.rename_column('subscription', 'tilde_subscription')
        tilde_subscriptions.set_columns(columns)
        return tilde_subscriptions

    @staticmethod
    def _build_tilde_publisher(
        data: Ros2DataModel,
    ) -> TracePointData:
        columns = ['tilde_publisher', 'tilde_subscription', 'node_name', 'topic_name']

        tilde_publishers = data.tilde_publishers.clone()
        tilde_publishers.reset_index()
        tilde_publishers.rename_column('publisher', 'tilde_publisher')
        tilde_publishers.set_columns(columns)
        return tilde_publishers

    @staticmethod
    def _build_tilde_sub_id(
        data: Ros2DataModel,
        sub: TracePointData
    ) -> TracePointData:
        columns = ['subscription_id', 'tilde_subscription', 'node_name', 'topic_name']
        tilde_subscribe_added = data.tilde_subscribe_added.clone()
        tilde_subscribe_added.reset_index()
        tilde_subscribe_added.merge(sub, ['node_name', 'topic_name'], how='left')

        tilde_subscribe_added.set_columns(columns)
        return tilde_subscribe_added

    @staticmethod
    def _build_tilde_sub(
        data: Ros2DataModel,
    ) -> TracePointData:
        columns = [
            'callback_id', 'callback_object', 'callback_object_intra', 'node_handle',
            'subscription_handle', 'callback_group_addr', 'topic_name', 'symbol', 'depth'
        ]

        def callback_id(row: pd.Series) -> str:
            cb_object = row['callback_object']
            return f'subscription_callback_{cb_object}'

        subscriptions = data.subscriptions.clone()
        subscriptions.reset_index()

        callback_objects = DataFrameFormatted._format_subscription_callback_object(data)
        merge(subscriptions, callback_objects, 'subscription_handle')

        symbols = data.callback_symbols.clone()
        symbols.reset_index()
        merge(subscriptions, symbols, 'callback_object')

        cbg = data.callback_group_subscription.clone()
        cbg.reset_index()
        merge(subscriptions, cbg, 'subscription_handle')

        subscriptions.add_column('callback_id', callback_id)

        subscriptions.set_columns(columns)

        return subscriptions

    @staticmethod
    def _is_ignored_subscription(
        data: Ros2DataModel,
        subscription_handle: int
    ) -> bool:
        sub_df = data.subscriptions.clone()
        sub_df.filter_rows('index', subscription_handle)
        nodes = data.nodes.clone()
        nodes.reset_index()
        merge(sub_df, nodes, 'node_handle')

        try:
            ns = sub_df.at(0, 'namespace')
            name = sub_df.at(0, 'name')
            topic_name = sub_df.at(0, 'topic_name')

            if ns == '/' and name == 'rviz2':
                return True

            if topic_name == '/parameter_events':
                return True
        except KeyError:
            pass
        return False

    @staticmethod
    def _format_subscription_callback_object(
        data: Ros2DataModel,
    ) -> TracePointData:
        """
        Split the callback_object of the subscription callback.

        into callback_object and callback_object_intra.

        Parameters
        ----------
        data : Ros2DataModel
            data

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
        callback_objects = data.callback_objects.clone()
        callback_objects.reset_index()
        callback_objects.rename_column('reference', 'subscription')
        subscription_objects = data.subscription_objects.clone()
        subscription_objects.reset_index()

        # Leave timestamp of rclcpp_subscription_callback_added trace point.
        subscription_objects.drop_column('timestamp')
        merge(subscription_objects, callback_objects, 'subscription',
              merge_drop_columns=['tid', 'rmw_handle'])

        ret_data = TracePointIntermediateData(
            ['subscription_handle', 'callback_object', 'callback_object_intra'],
            {'callback_object_intra': 'Int64'}
        )
        for key, group in subscription_objects.df.groupby('subscription_handle'):
            group.reset_index(drop=True, inplace=True)

            subscription_handle = int(key)  # type: ignore
            if DataFrameFormatted._is_ignored_subscription(data, subscription_handle):
                continue

            record = {
                'subscription_handle': key,
            }
            if len(group) == 1:
                record['callback_object'] = group.at[0, 'callback_object']
            elif len(group) == 2:
                # NOTE:
                # The smaller timestamp is the callback_object of the in-process communication.
                # The larger timestamp is callback_object for inter-process communication.
                group.sort_values('timestamp', inplace=True)
                group.reset_index(drop=True, inplace=True)
                record['callback_object'] = group.at[1, 'callback_object']
                record['callback_object_intra'] = group.at[0, 'callback_object']
            else:
                cb_objs = group['callback_object'].values
                logger.warning(
                    'More than three callbacks are registered in one subscription_handle. '
                    'Skip loading callback info. The following callbacks cannot be measured.'
                    f'subscription_handle = {key}, '
                    f'callback_objects = {cb_objs}')
            ret_data.append(record)
        return ret_data.get_finalized()

    @staticmethod
    def _build_nodes(
        data: Ros2DataModel
    ) -> TracePointData:
        columns = ['node_id', 'node_handle', 'node_name']

        node = data.nodes.clone()
        node.reset_index()

        def ns_and_node_name(row: pd.Series) -> str:
            ns: str = row['namespace']
            name: str = row['name']

            if ns[-1] == '/':
                return ns + name
            else:
                return ns + '/' + name

        node.add_column('node_name', ns_and_node_name)

        def to_node_id(row: pd.Series) -> str:
            node_name = row['node_name']
            node_handle = row['node_handle']
            return f'{node_name}_{node_handle}'

        node.add_column('node_id', to_node_id)
        node.set_columns(columns)
        return node


def merge(
    left_data: TracePointData,
    right_data: TracePointData,
    on,
    how: Optional[str] = None,
    merge_drop_columns: Optional[List[str]] = None
):
    how = how or 'inner'
    merge_drop_columns = merge_drop_columns or ['timestamp', 'tid', 'rmw_handle']
    left_data.merge(right_data, on, how=how, drop_columns=merge_drop_columns)
