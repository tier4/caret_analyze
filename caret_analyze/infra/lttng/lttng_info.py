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
from logging import getLogger
from typing import (
    Any,
    Callable,
    DefaultDict,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Union,
)

import numpy as np
import pandas as pd

from .ros2_tracing.data_model import Ros2DataModel
from .value_objects import (
    CallbackGroupValueLttng,
    NodeValueLttng,
    PublisherValueLttng,
    SubscriptionCallbackValueLttng,
    TimerCallbackValueLttng,
    TimerControl,
    TimerInit,
    TransformBroadcasterValueLttng,
    TransformBufferValueLttng,
    ServiceCallbackValueLttng,
    ClientCallbackValueLttng,
)
from ...common import Util
from ...exceptions import InvalidArgumentError
from ...value_objects import (
    ExecutorValue,
    NodeValue,
    Qos,
    TimerValue,
    TransformValue
)

logger = getLogger(__name__)


class Collection(Iterable):

    def __init__(self) -> None:
        self._data: DefaultDict = defaultdict(list)

    def add(self, node_id: int, val: Any) -> None:
        self._data[node_id].append(val)

    def gets(self, node: NodeValueLttng) -> List[Any]:
        return self._data[node.node_id]

    def __iter__(self) -> Iterator[Any]:
        return iter(Util.flatten(self._data.values()))


class Callbacks():

    def __init__(
        self,
        timer_cbs: Collection,
        sub_cbs: Collection,
    ) -> None:
        self._timer_cbs = timer_cbs
        self._sub_cbs = sub_cbs

    def is_user_defined(
        self,
        callback_id: str
    ) -> bool:
        for cb in self._sub_cbs:
            assert isinstance(cb, SubscriptionCallbackValueLttng)
            if cb.callback_id == callback_id:
                return cb.subscribe_topic_name not in ['/clock', '/parameter_events']

        return False


class LttngInfo:

    def __init__(self, data: Ros2DataModel):
        self._formatted = DataFrameFormatted(data)

        # TODO(hsgwa): check rmw_impl for each process.
        self._rmw_implementation = data.rmw_impl.iloc[0, 0] if len(data.rmw_impl) > 0 else ''
        self._timer_cbs = LttngInfo._load_timer_cbs(self._formatted)
        self._sub_cbs = LttngInfo._load_sub_cbs(self._formatted)
        self._srv_cbs = LttngInfo._load_srv_cbs(self._formatted)
        self._clt_cbs = LttngInfo._load_clt_cbs(self._formatted)
        self._pubs = LttngInfo._load_pubs(self._formatted)
        callbacks = Callbacks(self._timer_cbs, self._sub_cbs)
        self._cbgs = LttngInfo._load_cbgs(self._formatted, callbacks)

        self._id_to_topic: Dict[str, str] = {}

    def get_rmw_impl(self) -> str:
        """
        Get rmw implementation.

        Returns
        -------
        str
            rmw_implementation

        """
        return self._rmw_implementation

    @staticmethod
    def _load_timer_cbs(formatted: pd.DataFrame) -> Collection:
        timer_cbs_info = Collection()

        timer_df = formatted.timer_callbacks_df
        timer_df = merge(timer_df, formatted.nodes_df, 'node_handle')

        for _, row in timer_df.iterrows():
            val = TimerCallbackValueLttng(
                    callback_id=row['callback_id'],
                    node_name=row['node_name'],
                    node_id=row['node_id'],
                    symbol=row['symbol'],
                    period_ns=row['period_ns'],
                    timer_handle=row['timer_handle'],
                    publish_topic_names=None,
                    callback_object=row['callback_object']
                )
            timer_cbs_info.add(row['node_id'], val)

        return timer_cbs_info

    @staticmethod
    def _load_srv_cbs(formatted: pd.DataFrame) -> Collection:
        cbs = Collection()
        service_df = formatted.service_callbacks_df
        service_df = merge(service_df, formatted.nodes_df, 'node_handle')

        for _, row in service_df.iterrows():
            val = ServiceCallbackValueLttng(
                    callback_id=row['callback_id'],
                    node_id=row['node_id'],
                    node_name=row['node_name'],
                    symbol=row['symbol'],
                    service_handle=row['service_handle'],
                    callback_object=row['callback_object'],
                    service_name=row['service_name'],
                )
            cbs.add(row['node_id'], val)

        return cbs

    def get_service_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[ServiceCallbackValueLttng]:
        node_lttng = self._get_node_lttng(node)
        return self._srv_cbs.gets(node_lttng)

    @staticmethod
    def _load_clt_cbs(formatted: pd.DataFrame) -> Collection:
        # not implemented yet
        return Collection()

    @lru_cache
    def get_tf_frames(self) -> Sequence[TransformValue]:
        nodes = self.get_nodes()
        tfs: List[TransformValue] = []
        for node in nodes:
            bf = self.get_tf_broadcaster(node)
            if bf is not None:
                tfs += bf.broadcast_transforms
        return tfs

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

        node_lttng = self._get_node_lttng(node)
        return self._timer_cbs.gets(node_lttng)

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
            if str(node_name).startswith('_ros2_cli'):
                continue
            node_id = row['node_id']
            node_handle = row['node_handle']
            if node_name in added_nodes:
                duplicate_nodes.add(node_name)
            added_nodes.add(node_name)
            nodes.append(NodeValueLttng(node_name, node_handle, node_id))

        for duplicate_node in duplicate_nodes:
            logger.warning(
                f'Duplicate node. node_name = {duplicate_node}. '
                'The measurement results may be incorrect.')

        return nodes

    def get_tf_buffer(
        self,
        node: NodeValue
    ) -> Optional[TransformBufferValueLttng]:
        nodes = self.get_nodes()

        listen_transforms = self.get_tf_frames()

        for tf_buffer_core, group in self._formatted.tf_buffers_df.groupby('tf_buffer_core'):
            transforms = []
            tf_map = self.get_tf_buffer_frame_compact_map(tf_buffer_core)
            for _, row in group.iterrows():
                frame_id = tf_map[row['frame_id_compact']]
                child_frame_id = tf_map[row['child_frame_id_compact']]
                transforms.append(TransformValue(frame_id, child_frame_id))

            row = group.iloc[0, :]

            if row['listener_node_id'] is None or row['lookup_node_id'] is None:
                continue

            listener_node = Util.find_one(lambda x: x.node_id == row['listener_node_id'], nodes)
            lookup_node = Util.find_one(lambda x: x.node_id == row['lookup_node_id'], nodes)
            if lookup_node.node_name != node.node_name:
                continue

            return TransformBufferValueLttng(
                    listener_node_id=listener_node.node_id,
                    listener_node_name=listener_node.node_name,
                    lookup_node_id=lookup_node.node_id,
                    lookup_node_name=lookup_node.node_name,
                    lookup_transforms=tuple(transforms),
                    listen_transforms=tuple(listen_transforms),
                    buffer_handler=tf_buffer_core
                )
        return None

    def _get_node_lttng(self, node: NodeValue) -> NodeValueLttng:
        nodes = self.get_nodes()
        return Util.find_one(lambda x: x.node_name == node.node_name, nodes)

    def get_tf_broadcaster(
        self,
        node: NodeValue
    ) -> Optional[TransformBroadcasterValueLttng]:
        node_lttng = self._get_node_lttng(node)
        pubs = self._get_tf_publishers_without_cb_bind(node_lttng.node_id)

        br_df = self._formatted.tf_broadcasters_df
        br_df = br_df[br_df['node_handle'] == node_lttng.node_handle]
        if len(br_df) == 0:
            return None

        pub_id = br_df['publisher_id'].values[0]
        transforms = tuple(
            TransformValue(tf['frame_id'], tf['child_frame_id'])
            for _, tf
            in br_df.iterrows()
        )
        br_pub = Util.find_one(lambda x: x.publisher_id == pub_id, pubs)
        transform_broadcaster = br_df['transform_broadcaster'].values[0]
        cb_ids = ()
        return TransformBroadcasterValueLttng(br_pub, transforms, cb_ids, transform_broadcaster)

    def get_tf_broadcaster_frame_compact_map(self, broadcaster: int) -> Dict[int, str]:
        maps = self._broadcaster_frame_comact_maps
        assert broadcaster in maps
        return maps[broadcaster]

    @cached_property
    def _broadcaster_frame_comact_maps(self) -> Dict[int, Dict[int, str]]:
        m: DefaultDict = defaultdict(dict)
        for _, row in self._formatted.tf_broadcaster_frame_id_df.iterrows():
            broadcater_map = m[row['tf_broadcaster']]
            broadcater_map[row['frame_id_compact']] = row['frame_id']
        return m

    def get_tf_buffer_frame_compact_map(self, buffer_core: int) -> Dict[int, str]:
        maps = self._buffer_frame_compact_maps
        return maps[buffer_core]

    @cached_property
    def _buffer_frame_compact_maps(self) -> Dict[int, Dict[int, str]]:
        m: DefaultDict = defaultdict(dict)
        for _, row in self._formatted.tf_buffer_frame_id_df.iterrows():
            buffer_map = m[row['tf_buffer_core']]
            buffer_map[row['frame_id_compact']] = row['frame_id']
        return m

    def _load_sub_cbs(
        formatted: pd.DataFrame
    ) -> Collection:
        sub_cbs_info = Collection()

        sub_df = formatted.subscription_callbacks_df
        sub_df = merge(sub_df, formatted.nodes_df, 'node_handle')
        tilde_sub = formatted.tilde_subscriptions_df
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

            val = SubscriptionCallbackValueLttng(
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
            sub_cbs_info.add(node_id, val)

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
        node_lttng = self._get_node_lttng(node)
        return self._sub_cbs.gets(node_lttng)

    @property
    def tilde_sub_id_map(self) -> Dict[int, int]:
        return self._formatted.tilde_sub_id_map

    # def _get_publishers(self, node: NodeValueLttng) -> List[PublisherValueLttng]:
        # node_id = node.node_id
        # if node_id not in self._binder_cache.keys():
        #     self._binder_cache[node_id] = PublisherBinder(self, self._source)

        # binder = self._binder_cache[node_id]
        # if not binder.can_bind(node):
        # return self.get_publishers_without_cb_bind(node_id)

        # cbs: List[Union[TimerCallbackValueLttng,
        #                 SubscriptionCallbackValueLttng]] = []
        # cbs += self.get_timer_callbacks(node)
        # cbs += self.get_subscription_callbacks(node)
        # pubs_info = self.get_publishers_without_cb_bind(node_id)

        # for i, pub_info in enumerate(pubs_info):
        #     topic_name = pub_info.topic_name
        #     cbs_pubs = Util.filter_items(
        #         lambda x: topic_name in x.publish_topic_names, cbs)
        #     cb_ids = tuple(c.callback_id for c in cbs_pubs)
        #     pubs_info[i] = PublisherValueLttng(
        #         node_name=pub_info.node_name,
        #         node_id=pub_info.node_id,
        #         topic_name=pub_info.topic_name,
        #         callback_ids=cb_ids,
        #         publisher_handle=pub_info.publisher_handle
        #     )

        # return pubs_info

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

        node_lttng = self._get_node_lttng(node)
        return self._pubs.gets(node_lttng)

    def _get_nodes(
        self,
        node_name: str
    ) -> Sequence[NodeValueLttng]:
        return Util.filter_items(lambda x: x.node_name == node_name, self.get_nodes())

    @staticmethod
    def _load_pubs(
        formatted: pd.DataFrame
    ) -> Collection:
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
        pubs = Collection()

        pub_df = formatted.publishers_df
        pub_df = merge(pub_df, formatted.nodes_df, 'node_handle')
        tilde_pub = formatted.tilde_publishers_df

        pub_df = pd.merge(pub_df, tilde_pub, on=['node_name', 'topic_name'], how='left')
        pub_df = pub_df.astype({'tilde_publisher': 'Int64'})

        for _, row in pub_df.iterrows():
            tilde_publisher = row['tilde_publisher']
            if tilde_publisher is pd.NA:
                tilde_publisher = None

            val = PublisherValueLttng(
                    node_name=row['node_name'],
                    topic_name=row['topic_name'],
                    node_id=row['node_id'],
                    callback_ids=None,
                    publisher_handle=row['publisher_handle'],
                    publisher_id=row['publisher_id'],
                    tilde_publisher=tilde_publisher
                )
            pubs.add(row['node_id'], val)

        return pubs

    def _get_tf_publishers_without_cb_bind(self, node_id: str) -> List[PublisherValueLttng]:
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
            if row['topic_name'] not in ['/tf', '/tf_static']:
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
                    publisher_id=row['publisher_id'],
                    tilde_publisher=tilde_publisher
                )
            )

        return pubs_info

    @staticmethod
    def _load_cbgs(
        formatted: pd.DataFrame,
        callbacks: Callbacks
    ) -> Collection:
        cbgs = Collection()

        concate_target_dfs = []
        concate_target_dfs.append(formatted.timer_callbacks_df)
        concate_target_dfs.append(formatted.subscription_callbacks_df)
        concate_target_dfs.append(formatted.service_callbacks_df)

        column_names = [
            'callback_group_addr', 'callback_id', 'node_handle'
        ]
        concat_df = concat(column_names, concate_target_dfs)

        concat_df = merge(
            concat_df, formatted.nodes_df, 'node_handle')
        concat_df = merge(
            concat_df, formatted.callback_groups_df, 'callback_group_addr')

        for _, group_df in concat_df.groupby(['callback_group_addr']):
            row = group_df.iloc[0, :]

            callback_ids = tuple(group_df['callback_id'].values)
            callback_ids = tuple(Util.filter_items(callbacks.is_user_defined, callback_ids))

            val = CallbackGroupValueLttng(
                    callback_group_type_name=row['group_type_name'],
                    node_name=row['node_name'],
                    node_id=row['node_id'],
                    callback_ids=callback_ids,
                    callback_group_id=row['callback_group_id'],
                    callback_group_addr=row['callback_group_addr'],
                    executor_addr=row['executor_addr'],
                )

            cbgs.add(row['node_id'], val)

        return cbgs

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
        node_lttng = self._get_node_lttng(node)
        return self._cbgs.gets(node_lttng)

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

        for i, (_, group) in enumerate(exec_df.groupby('executor_addr')):
            row = group.iloc[0, :]
            executor_type_name = row['executor_type_name']

            cbg_ids = group['callback_group_id'].values
            executor_id = f'executor_{i}'
            execs.append(
                ExecutorValue(
                    executor_id,
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
        self._tf_broadcasters = self._build_tf_broadcasters_df(data, self._pub_df)
        self._tf_buffers = self._build_tf_buffers_df(
            data,
            self._nodes_df,
            self._timer_callbacks_df,
            self._sub_callbacks_df,
            self._srv_callbacks_df,
        )
        self._broadcaster_frame_id_df = data.broadcaster_frame_id_compact.reset_index()
        self._buffer_frame_id_df = data.buffer_frame_id_compact.reset_index()

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
    def tf_broadcasters_df(self) -> pd.DataFrame:
        return self._tf_broadcasters

    @property
    def tf_broadcaster_frame_id_df(self) -> pd.DataFrame:
        return self._broadcaster_frame_id_df

    @property
    def tf_buffer_frame_id_df(self) -> pd.DataFrame:
        return self._buffer_frame_id_df

    @property
    def service_callbacks_df(self) -> pd.DataFrame:
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
            - callback_group_addr
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

    @property
    def tf_buffers_df(self):
        return self._tf_buffers

    @staticmethod
    def _build_tf_buffers_df(
        data: Ros2DataModel,
        node_df: pd.DataFrame,
        timer_df: pd.DataFrame,
        sub_df: pd.DataFrame,
        srv_df: pd.DataFrame
    ) -> pd.DataFrame:
        columns = [
            'tf_buffer',
            'tf_buffer_core',
            'callback',
            'clock',
            'listener_node_id',
            'lookup_node_id',
            'frame_id_compact',
            'child_frame_id_compact',
        ]
        try:
            df = data.construct_tf_buffer
            df = pd.merge(
                df,
                data.init_bind_tf_buffer_core,
                left_on='tf_buffer_core',
                right_on='tf_buffer_core'
            )

            lookup_df = data.tf_buffer_lookup_transforms
            df = pd.merge(
                df,
                lookup_df,
                on='tf_buffer_core',
            )

            cb_columns = ['callback_object', 'node_handle']
            cb_df = pd.concat(
                [
                    timer_df[cb_columns],
                    sub_df[cb_columns],
                    srv_df[cb_columns]
                ]
            )
            df = pd.merge(
                df, cb_df,
                left_on='callback',
                right_on='callback_object',
                how='left'
            )

            df = pd.merge(
                df,
                node_df,
                on='node_handle',
                how='left'
            )
            df.rename(
                {'node_id': 'listener_node_id', 'node_handle': 'listener_node_handle'},
                axis=1, inplace=True
            )

            df = pd.merge(
                df,
                data.construct_node_hook,
                left_on='clock',
                right_on='clock',
                how='left'
            )

            df = pd.merge(
                df,
                node_df,
                on='node_handle',
                how='left'
            )
            df.dropna(inplace=True)

            df.rename(
                {'node_id': 'lookup_node_id', 'node_handle': 'lookup_node_handle'},
                axis=1, inplace=True)
        except KeyError:
            pass

        df = DataFrameFormatted._ensure_columns(df, columns)
        return df[columns].astype('object')

    @staticmethod
    def _build_publisher_df(
        data: Ros2DataModel,
    ) -> pd.DataFrame:
        columns = [
            'publisher_id',
            'publisher_handle',
            'node_handle',
            'topic_name',
            'depth',
        ]
        df = data.publishers

        def to_publisher_id(i: int, row: pd.Series):
            return f'publisher_{i}'

        df = DataFrameFormatted._add_column(df, 'publisher_id', to_publisher_id)
        df = DataFrameFormatted._ensure_columns(df, columns)
        return df[columns].astype('object')

    @staticmethod
    def _build_tf_broadcasters_df(
        data: Ros2DataModel,
        pub_df: pd.DataFrame
    ) -> pd.DataFrame:
        columns = [
            'transform_broadcaster',
            'publisher_id',
            'publisher_handle',
            'node_handle',
            'topic_name',
            'depth',
            'frame_id',
            'child_frame_id'
        ]
        try:
            df = data.transform_broadcaster.reset_index()
            tf_frames = data.transform_broadcaster_frames
            df = pd.merge(
                df,
                tf_frames,
                on='transform_broadcaster',
                how='outer'
            )

            df = pd.merge(df, pub_df, on='publisher_handle', how='left')
        except KeyError:
            pass
        df = DataFrameFormatted._ensure_columns(df, columns)
        return df[columns].astype('object')

    @staticmethod
    def _build_timer_control_df(
        data: Ros2DataModel,
    ) -> pd.DataFrame:
        columns = ['timestamp', 'timer_handle', 'type', 'params']
        df = data.timers
        df['type'] = 'init'
        df['params'] = [{'period': row['period']} for (_, row) in df.iterrows()]
        df = DataFrameFormatted._ensure_columns(df, columns)
        return df[columns].astype('object')

    @staticmethod
    def _build_executor_df(
        data: Ros2DataModel,
    ) -> pd.DataFrame:
        columns = ['executor_id', 'executor_addr', 'executor_type_name']

        df = data.executors

        df_ = data.executors_static
        if len(df_) > 0:
            columns_ = columns[1:]  # ignore executor_id
            df = concat(columns_, [df, df_])

        def to_executor_id(i: int, row: pd.Series) -> str:
            return f'executor_{i}'

        df = DataFrameFormatted._add_column(df, 'executor_id', to_executor_id)
        df = DataFrameFormatted._ensure_columns(df, columns)

        df = df[columns]

        # data.callback_groups returns duplicate results that differ only in timestamp.
        # Remove duplicates to make it unique.
        df.drop_duplicates(inplace=True)

        return df[columns].astype('object')

    @staticmethod
    def _build_cbg_df(
        data: Ros2DataModel,
    ) -> pd.DataFrame:
        columns = ['callback_group_id', 'callback_group_addr', 'group_type_name', 'executor_addr']
        df = data.callback_groups

        df_static = data.callback_groups_static
        df_static_exec = data.executors_static

        if len(df_static) > 0 and len(df_static_exec) > 0:
            df_static = merge(df_static, df_static_exec, 'entities_collector_addr')
            columns_ = columns[1:]  # ignore callback_group_id
            df = concat(columns_, [df, df_static])

        def to_callback_group_id(i: int, row: pd.Series) -> str:
            return f'callback_group_{i}'

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
                exec_addr = list(set(group['executor_addr'].values))
                msg += f'executor address: {exec_addr}'
                logger.warn(msg)
                executor_duplicated_indexes += list(group.index)[:-1]

        if len(executor_duplicated_indexes) >= 1:
            df.drop(index=executor_duplicated_indexes, inplace=True)

        df.reset_index(drop=True, inplace=True)
        return df.astype('object')

    @staticmethod
    def _build_timer_callbacks_df(
        data: Ros2DataModel,
    ) -> pd.DataFrame:
        columns = [
            'callback_id', 'callback_object', 'node_handle', 'timer_handle', 'callback_group_addr',
            'period_ns', 'symbol',
        ]

        def callback_id(i: int, row: pd.Series) -> str:
            return f'timer_callback_{i}'
        try:
            df = data.timers

            timer_node_links_df = data.timer_node_links
            df = merge(df, timer_node_links_df, 'timer_handle')

            callback_objects_df = data.callback_objects.rename(
                columns={'reference': 'timer_handle'})
            df = merge(df, callback_objects_df, 'timer_handle')

            symbols_df = data.callback_symbols
            df = merge(df, symbols_df, 'callback_object')

            cbg = data.callback_group_timer
            df = merge(df, cbg, 'timer_handle')

            df = DataFrameFormatted._add_column(df, 'callback_id', callback_id)

            df.rename({'period': 'period_ns'}, inplace=True, axis=1)

            df = DataFrameFormatted._ensure_columns(df, columns)

            return df[columns].astype('object')
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

        def callback_id(i: int, row: pd.Series) -> str:
            return f'subscription_callback_{i}'

        try:
            df = data.subscriptions

            callback_objects_df = DataFrameFormatted._format_subscription_callback_object(data)
            df = merge(df, callback_objects_df, ['pid', 'subscription_handle'])

            symbols_df = data.callback_symbols
            df = merge(df, symbols_df, ['pid', 'callback_object'])

            cbg = data.callback_group_subscription
            df = merge(df, cbg, 'subscription_handle')

            df = DataFrameFormatted._add_column(
                df, 'callback_id', callback_id
            )

            df = DataFrameFormatted._ensure_columns(df, columns)

            return df[columns].astype('object')
        except KeyError:
            return pd.DataFrame(columns=columns).astype('object')

    @staticmethod
    def _build_srv_callbacks_df(
        data: Ros2DataModel,
    ) -> pd.DataFrame:
        columns = [
            'callback_id', 'callback_object', 'node_handle',
            'service_handle', 'callback_group_addr', 'service_name', 'symbol'
        ]

        def callback_id(i: int, row: pd.Series) -> str:
            return f'service_callback_{i}'

        try:
            df = data.services

            callback_objects_df = data.callback_objects.rename(
                {'reference': 'service_handle'}, axis=1)
            df = merge(df, callback_objects_df, ['pid', 'service_handle'])

            symbols_df = data.callback_symbols
            df = merge(df, symbols_df, ['callback_object', 'pid'])

            cbg = data.callback_group_service
            df = merge(df, cbg, ['pid', 'service_handle'])

            df = DataFrameFormatted._add_column(
                df, 'callback_id', callback_id
            )

            df = DataFrameFormatted._ensure_columns(df, columns)

            return df[columns].astype('object')
        except KeyError:
            return pd.DataFrame(columns=columns)

    @staticmethod
    def _build_tilde_subscription_df(
        data: Ros2DataModel,
    ) -> pd.DataFrame:
        columns = ['tilde_subscription', 'node_name', 'topic_name']

        try:
            df = data.tilde_subscriptions

            df.rename({'subscription': 'tilde_subscription'}, axis=1, inplace=True)
            df = DataFrameFormatted._ensure_columns(df, columns)

            return df[columns].astype('object')
        except KeyError:
            return pd.DataFrame(columns=columns)

    @staticmethod
    def _build_tilde_publisher_df(
        data: Ros2DataModel,
    ) -> pd.DataFrame:
        columns = ['tilde_publisher', 'tilde_subscription', 'node_name', 'topic_name']

        try:
            df = data.tilde_publishers

            df.rename({'publisher': 'tilde_publisher'}, axis=1, inplace=True)
            df = DataFrameFormatted._ensure_columns(df, columns)

            return df[columns].astype('object')
        except KeyError:
            return pd.DataFrame(columns=columns)

    @staticmethod
    def _build_tilde_sub_id_df(
        data: Ros2DataModel,
        sub_df: pd.DataFrame
    ) -> pd.DataFrame:
        columns = ['subscription_id', 'tilde_subscription', 'node_name', 'topic_name']
        try:
            df = data.tilde_subscribe_added
            df = pd.merge(df, sub_df, on=['node_name', 'topic_name'], how='left')

            df = DataFrameFormatted._ensure_columns(df, columns)

            return df[columns].astype('object')
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

        def callback_id(i: int, row: pd.Series) -> str:
            return f'subscription_callback_{i}'

        try:
            df = data.subscriptions

            callback_objects_df = DataFrameFormatted._format_subscription_callback_object(data)
            df = merge(df, callback_objects_df, 'subscription_handle')

            symbols_df = data.callback_symbols
            df = merge(df, symbols_df, 'callback_object')

            cbg = data.callback_group_subscription
            df = merge(df, cbg, 'subscription_handle')

            df = DataFrameFormatted._add_column(
                df, 'callback_id', callback_id
            )

            df = DataFrameFormatted._ensure_columns(df, columns)

            return df[columns].astype('object')
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
        callback_objects_df = data.callback_objects.rename(
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
        cell_rule: Callable[[int, pd.Series], str]
    ) -> pd.DataFrame:
        from copy import deepcopy
        df_ = deepcopy(df)

        df_[column_name] = ''
        for i in range(len(df_)):
            row = df_.iloc[i, :]
            df_.loc[i, column_name] = cell_rule(i, row)

        return df_

    @staticmethod
    def _build_nodes_df(
        data: Ros2DataModel
    ) -> pd.DataFrame:
        columns = ['node_id', 'node_handle', 'node_name']

        node_df = data.nodes

        def ns_and_node_name(i: int, row: pd.Series) -> str:
            ns: str = row['namespace']
            name: str = row['name']

            if ns[-1] == '/':
                return ns + name
            else:
                return ns + '/' + name

        node_df = DataFrameFormatted._add_column(node_df, 'node_name', ns_and_node_name)

        def to_node_id(i: int, row: pd.Series) -> str:
            node_name = row['node_name']
            return f'{node_name}_{i}'

        node_df = DataFrameFormatted._add_column(node_df, 'node_id', to_node_id)

        node_df.drop(['namespace', 'name'], inplace=True, axis=1)

        node_df.reset_index(drop=True, inplace=True)
        return node_df[columns].astype('object')


def merge(
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    join_key: Union[str, List[str]],
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
            raise InvalidArgumentError('')
        concat_targets.append(df[column_names])

    return pd.concat(concat_targets, axis=0).reset_index(drop=True)
