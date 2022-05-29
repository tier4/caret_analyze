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
    Set,
    Union,
)

import numpy as np
import pandas as pd

from .column_names import COLUMN_NAME as CN
from .ros2_tracing.data_model import Ros2DataModel
from .value_objects import (
    CallbackGroupValueLttng,
    ClientCallbackValueLttng,
    ExecutorValueLttng,
    IntraProcessBufferValueLttng,
    NodeValueLttng,
    PublisherValueLttng,
    ServiceCallbackValueLttng,
    SubscriptionCallbackValueLttng,
    SubscriptionValueLttng,
    TimerCallbackValueLttng,
    TransformBroadcasterValueLttng,
    TransformBufferValueLttng,
)
from ...common import Util
from ...exceptions import (
    Error,
    InvalidArgumentError,
    ItemNotFoundError,
    MultipleItemFoundError,
)
from ...record import (
    ColumnAttribute,
    ColumnMapper,
    ColumnValue,
    merge,
    merge_sequencial,
    RecordFactory,
    RecordInterface,
    RecordsFactory,
    RecordsInterface,
)
from ...value_objects import (
    BroadcastedTransformValue,
    NodeValue,
    Qos,
    TimerValue,
    TransformValue,
)

logger = getLogger(__name__)


class Collection(Iterable):

    def __init__(self) -> None:
        self._data: DefaultDict = defaultdict(list)

    def add(self, node_id: str, val: Any) -> None:
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
        mapper = data.rmw_implementation.columns.get('rmw_impl').mapper
        self._rmw_implementation = mapper.get(data.rmw_implementation.data[0].get('rmw_impl')) \
            if len(data.rmw_implementation) > 0 else None
        self._timer_cbs = LttngInfo._load_timer_cbs(self._formatted)
        self._sub_cbs = LttngInfo._load_sub_cbs(self._formatted)
        self._srv_cbs = LttngInfo._load_srv_cbs(self._formatted)
        # self._clt_cbs = LttngInfo._load_clt_cbs(self._formatted)
        self._pubs = LttngInfo._load_pubs(self._formatted)
        self._subs = LttngInfo._load_subs(self._formatted)
        callbacks = Callbacks(self._timer_cbs, self._sub_cbs)
        self._cbgs = LttngInfo._load_cbgs(self._formatted, callbacks)
        self._ipc_buffers = LttngInfo._load_ipc_buffers(self._formatted)
        self._tf_buffers = self._load_tf_buffers(self._formatted)

        self._id_to_topic: Dict[str, str] = {}

        self._intra_pub_handles = set(
            data.rclcpp_intra_publish.get_column_series(CN.PUBLISHER_HANDLE)
        )
        intra_cb_objects = set(
            data.dispatch_intra_process_subscription_callback.get_column_series(
                CN.CALLBACK_OBJECT))
        sub_df = self._formatted.subscription_callbacks_df
        sub_df.filter_if(
            lambda record: record.get_with_default(CN.CALLBACK_OBJECT_INTRA, 0) in intra_cb_objects
        )
        self._intra_sub_handles = {
            record.get(CN.SUBSCRIPTION_HANDLE) for record in sub_df.data
        }

    def get_rmw_impl(self) -> Optional[str]:
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
        timer_df = merge(
            timer_df,
            formatted.nodes_df,
            CN.NODE_HANDLE,
            CN.NODE_HANDLE,
            how='inner'
        )

        for i in range(len(timer_df)):
            val = TimerCallbackValueLttng(
                    pid=timer_df.get(i, CN.PID),
                    callback_id=timer_df.get(i, CN.CALLBACK_ID),
                    node_name=timer_df.get(i, CN.NODE_NAME),
                    node_id=timer_df.get(i, CN.NODE_ID),
                    symbol=timer_df.get(i, CN.SYMBOL),
                    period_ns=timer_df.get(i, CN.PERIOD_NS),
                    timer_handle=timer_df.get(i, CN.TIMER_HANDLE),
                    publish_topic_names=None,
                    callback_object=timer_df.get(i, CN.CALLBACK_OBJECT)
                )
            timer_cbs_info.add(timer_df.get(i, CN.NODE_ID), val)

        return timer_cbs_info

    @staticmethod
    def _load_srv_cbs(formatted: pd.DataFrame) -> Collection:
        cbs = Collection()
        service_df = formatted.service_callbacks_df
        service_df = merge(
            service_df,
            formatted.nodes_df,
            CN.NODE_HANDLE,
            CN.NODE_HANDLE,
            'inner'
        )

        for i in range(len(service_df)):
            val = ServiceCallbackValueLttng(
                    pid=service_df.get(i, CN.PID),
                    callback_id=service_df.get(i, CN.CALLBACK_ID),
                    node_id=service_df.get(i, CN.NODE_ID),
                    node_name=service_df.get(i, CN.NODE_NAME),
                    symbol=service_df.get(i, CN.SYMBOL),
                    service_handle=service_df.get(i, CN.SERVICE_HANDLE),
                    callback_object=service_df.get(i, CN.CALLBACK_OBJECT),
                    service_name=service_df.get(i, CN.SERVICE_NAME),
                )
            cbs.add(service_df.get(i, CN.NODE_ID), val)

        return cbs

    @staticmethod
    def _load_ipc_buffers(formatted: DataFrameFormatted) -> Collection:
        buffers = Collection()
        buffer_df = formatted.ipc_buffers_df

        for i in range(len(buffer_df)):
            pid = buffer_df.iget(i, CN.PID)
            node_name = buffer_df.sget(i, CN.NODE_NAME)
            topic_name = buffer_df.sget(i, CN.TOPIC_NAME)
            buffer = buffer_df.iget(i, CN.BUFFER)
            node_id = buffer_df.sget(i, CN.NODE_ID)
            assert pid is not None
            assert node_name is not None
            assert topic_name is not None
            assert buffer is not None
            assert node_id is not None

            val = IntraProcessBufferValueLttng(
                pid, node_name, topic_name, buffer
            )
            buffers.add(node_id, val)

        return buffers

    def _load_tf_buffers(self, formatted: DataFrameFormatted) -> Collection:
        buffers = Collection()
        nodes = self.get_nodes()
        listen_transforms = self.get_tf_frames()

        for keys, group in formatted.tf_buffers_df.groupby([CN.TF_BUFFER_CORE]).items():
            tf_buffer_core = keys[0]

            try:
                transforms = []
                for i in range(len(group)):
                    source_frame_id = group.sget(i, CN.SOURCE_FRAME_ID)
                    target_frame_id = group.sget(i, CN.TARGET_FRAME_ID)
                    assert source_frame_id is not None
                    assert target_frame_id is not None
                    transforms.append(TransformValue(source_frame_id, target_frame_id))

                if group.sget(i, 'listener_node_id') is None \
                        or group.sget(i, 'lookup_node_id') is None:
                    continue

                listener_node = Util.find_one(
                    lambda x: x.node_id == group.sget(i, 'listener_node_id'), nodes)
                lookup_node = Util.find_one(
                    lambda x: x.node_id == group.sget(i, 'lookup_node_id'), nodes)

                listener_callbacks = self.get_subscription_callbacks(listener_node)
                listener_callback = [
                    cb
                    for cb
                    in listener_callbacks
                    if isinstance(cb, SubscriptionCallbackValueLttng) and
                    cb.subscribe_topic_name == '/tf'
                ][0]

                buffers.add(
                    lookup_node.node_id,
                    TransformBufferValueLttng(
                        pid=listener_node.pid,
                        listener_node_id=listener_node.node_id,
                        listener_node_name=listener_node.node_name,
                        lookup_node_id=lookup_node.node_id,
                        lookup_node_name=lookup_node.node_name,
                        lookup_transforms=tuple(transforms),
                        listen_transforms=tuple(listen_transforms),
                        buffer_handler=tf_buffer_core,
                        listener_callback=listener_callback
                    )
                )
            except Error as e:
                logger.warning(f'Failed to get tf_buffer. skip loading. {e}')
        return buffers

    def get_service_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[ServiceCallbackValueLttng]:
        node_lttng = self._get_node_lttng(node)
        return self._srv_cbs.gets(node_lttng)

    @staticmethod
    def _load_clt_cbs(formatted: DataFrameFormatted) -> Collection:
        cbs = Collection()
        return cbs

    def get_client_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[ClientCallbackValueLttng]:
        return []

    def get_ipc_buffers(
        self,
        node: NodeValue
    ) -> Sequence[IntraProcessBufferValueLttng]:
        node_lttng = self._get_node_lttng(node)
        return self._ipc_buffers.gets(node_lttng)

    @lru_cache
    def get_tf_frames(self) -> Sequence[BroadcastedTransformValue]:
        tfs: Set[BroadcastedTransformValue] = set()
        records = self._formatted.tf_frames_df
        for i in range(len(records)):
            frame_id = records.sget(i, 'frame_id')
            child_frame_id = records.sget(i, 'child_frame_id')
            assert frame_id is not None
            assert child_frame_id is not None
            tfs.add(BroadcastedTransformValue(frame_id, child_frame_id))
        return list(tfs)

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

        for i in range(len(nodes_df)):
            node_name = nodes_df.sget(i, 'node_name')
            if node_name is None:
                continue

            # Check LD_PRELOAD setting
            lib_caret_version = nodes_df.sget(i, 'lib_caret_version')
            if not lib_caret_version:
                msg = ('Failed to found trace point added by LD_PRELOAD. '
                       'Measurement results will not be correct. '
                       'The measurement may have been performed without setting LD_PRELOAD.')
                logger.warning(msg)

            if str(node_name).startswith('_ros2_cli'):
                continue

            if node_name in added_nodes:
                duplicate_nodes.add(node_name)
                continue
            added_nodes.add(node_name)

            pid = nodes_df.iget(i, 'pid')
            node_id = nodes_df.sget(i, 'node_id')
            node_handle = nodes_df.iget(i, 'node_handle')
            assert pid is not None
            assert node_id is not None
            assert node_handle is not None

            nodes.append(
                NodeValueLttng(pid, node_name, node_handle, node_id, lib_caret_version)
            )

        for duplicate_node in duplicate_nodes:
            logger.warning(
                f'Duplicate node. node_name = {duplicate_node}. '
                'The measurement results may be incorrect.')

        return nodes

    @lru_cache
    def get_node(self, node_name: str) -> NodeValue:
        nodes = self.get_nodes()
        for node in nodes:
            if node.node_name == node_name:
                return node
        raise ItemNotFoundError('Failed to get node. node_name = {}'.format(node_name))

    def get_tf_buffer(
        self,
        node: NodeValue
    ) -> Optional[TransformBufferValueLttng]:
        node_lttng = self._get_node_lttng(node)
        bufs = self._tf_buffers.gets(node_lttng)
        if len(bufs) == 0:
            return None
        if len(bufs) == 1:
            return bufs[0]
        raise MultipleItemFoundError(
            'Multiple tf_buffer found. node_name = {}'.format(node.node_name)
        )

    @lru_cache
    def _get_node_lttng(self, node: NodeValue) -> NodeValueLttng:
        nodes = self.get_nodes()
        return Util.find_one(lambda x: x.id_value == node.id_value, nodes)

    @lru_cache
    def get_tf_broadcaster(
        self,
        node: NodeValue
    ) -> Optional[TransformBroadcasterValueLttng]:
        node_lttng = self._get_node_lttng(node)
        pubs = self._get_tf_publishers_without_cb_bind(node_lttng.node_id)

        br_df = self._formatted.tf_broadcasters_df.to_dataframe()
        br_df = br_df[br_df['node_handle'] == node_lttng.node_handle]
        if len(br_df) == 0:
            return None

        pub_id = br_df['publisher_id'].values[0]
        transforms = tuple(
            BroadcastedTransformValue(tf['frame_id'], tf['child_frame_id'])
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

    def is_intra_process_communication(
        self,
        publishers: Sequence[PublisherValueLttng],
        subscription: SubscriptionValueLttng,
    ) -> bool:
        df = self._formatted.ipm_df
        for publisher in publishers:
            df.filter_if(
                lambda record:
                    record.get('publisher_handle') == publisher.publisher_handle and
                    record.get(
                        'subscription_handle') == subscription.subscription_handle
            )

            if len(df) > 0:
                return True

            if publisher.publisher_handle in self._intra_pub_handles and \
                    subscription.subscription_handle in self._intra_sub_handles:
                return True
        return False

    def _get_pub_lttng(self, node_name: str, topic_name: str) -> PublisherValueLttng:
        node = self._get_node_lttng(NodeValue(node_name, ''))

        for pub in self.get_publishers(node):
            if pub.topic_name == topic_name:
                return pub

        raise ItemNotFoundError('')

    def _get_sub_lttng(self, node_name: str, topic_name: str) -> SubscriptionValueLttng:
        node = self._get_node_lttng(NodeValue(node_name, ''))

        for sub in self.get_subscriptions(node):
            if sub.topic_name == topic_name:
                return sub

        raise ItemNotFoundError('')

    def get_subscriptions(self, node: NodeValue) -> List[SubscriptionValueLttng]:
        node_lttng = self._get_node_lttng(node)
        return self._subs.gets(node_lttng)

    @staticmethod
    def _load_subs(formatted: DataFrameFormatted) -> Collection:
        sub_df = formatted.subscription_callbacks_df
        sub_df = merge(
            sub_df,
            formatted.nodes_df,
            'node_handle',
            'node_handle',
            'inner'
        )
        tilde_sub = formatted.tilde_subscriptions_df
        sub_df = merge(
            sub_df,
            tilde_sub,
            ['node_name', 'topic_name'],
            ['node_name', 'topic_name'],
            how='left'
        )

        subs_info = Collection()

        for i in range(len(sub_df)):
            node_name = sub_df.sget(i, 'node_name')
            node_id = sub_df.sget(i, 'node_id')
            tilde_subscription = sub_df.iget(i, 'tilde_subscription')
            pid = sub_df.iget(i, 'pid')
            callback_id = sub_df.sget(i, 'callback_id')
            topic_name = sub_df.sget(i, 'topic_name')
            subscription_handle = sub_df.iget(i, 'subscription_handle')

            assert node_name is not None
            assert node_id is not None
            assert pid is not None
            assert callback_id is not None
            assert topic_name is not None
            assert subscription_handle is not None

            val = SubscriptionValueLttng(
                pid=pid,
                node_id=node_id,
                node_name=node_name,
                callback_id=callback_id,
                topic_name=topic_name,
                subscription_id='TODO',
                subscription_handle=subscription_handle,
                tilde_subscription=tilde_subscription
            )
            subs_info.add(node_id, val)

        return subs_info

    @cached_property
    def _broadcaster_frame_comact_maps(self) -> Dict[int, Dict[int, str]]:
        m: DefaultDict = defaultdict(dict)
        records = self._formatted.tf_broadcaster_frame_id_df
        for i in range(len(self._formatted.tf_broadcaster_frame_id_df)):
            tf_br = records.iget(i, 'tf_broadcaster')
            frame_id_compact = records.iget(i, 'frame_id_compact')
            frame_id = records.sget(i, 'frame_id')
            assert frame_id is not None
            assert tf_br is not None
            assert frame_id_compact is not None
            broadcater_map = m[tf_br]
            broadcater_map[frame_id_compact] = frame_id
        return m

    def get_tf_buffer_frame_compact_map(self, buffer_core: int) -> Dict[int, str]:
        maps = self._buffer_frame_compact_maps
        return maps[buffer_core]

    @cached_property
    def _buffer_frame_compact_maps(self) -> Dict[int, Dict[int, str]]:
        m: DefaultDict = defaultdict(dict)
        records = self._formatted.tf_buffer_frame_id_df
        for i in range(len(records)):
            tf_buff = records.iget(i, 'tf_buffer_core')
            compact = records.iget(i, 'frame_id_compact')
            frame_id = records.sget(i, 'frame_id')

            assert tf_buff is not None
            assert compact is not None
            assert frame_id is not None

            buffer_map = m[tf_buff]
            buffer_map[compact] = frame_id
        return m

    def _load_sub_cbs(
        formatted: pd.DataFrame
    ) -> Collection:
        sub_cbs_info = Collection()

        sub_df = formatted.subscription_callbacks_df
        sub_df = merge(
            sub_df,
            formatted.nodes_df,
            'node_handle',
            'node_handle',
            'inner'
        )
        tilde_sub = formatted.tilde_subscriptions_df
        sub_df = merge(
            sub_df,
            tilde_sub,
            ['node_name', 'topic_name'],
            ['node_name', 'topic_name'],
            how='left')

        for i in range(len(sub_df)):
            node_name = sub_df.get(i, 'node_name')
            node_id = sub_df.get(i, 'node_id')
            tilde_subscription = sub_df.get(i, 'tilde_subscription')

            val = SubscriptionCallbackValueLttng(
                    pid=sub_df.get(i, 'pid'),
                    callback_id=sub_df.get(i, 'callback_id'),
                    node_id=node_id,
                    node_name=node_name,
                    symbol=sub_df.get(i, 'symbol'),
                    subscribe_topic_name=sub_df.get(i, 'topic_name'),
                    publish_topic_names=None,
                    subscription_handle=sub_df.get(i, 'subscription_handle'),
                    callback_object=sub_df.get(i, 'callback_object'),
                    callback_object_intra=sub_df.get(i, 'callback_object_intra'),
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
        pub_df = merge(
            pub_df,
            formatted.nodes_df,
            'node_handle',
            'node_handle',
            'inner'
        )
        tilde_pub = formatted.tilde_publishers_df

        pub_df = merge(
            pub_df,
            tilde_pub,
            ['node_name', 'topic_name'],
            ['node_name', 'topic_name'],
            how='left')
        for i in range(len(pub_df)):
            tilde_publisher = pub_df.get(i, 'tilde_publisher')

            # Check whether publisher use caret-rclcpp
            caret_rclcpp_version = pub_df.get(i, 'caret_rclcpp_version')
            if not caret_rclcpp_version and pub_df.get(i, 'topic_name') != '/rosout':
                msg = ('caret-rclcpp is not used in following publishers:\n'
                       f'\tnode name: {pub_df.get(i, "node_name")},\n'
                       f'\ttopic name: {pub_df.get(i, "topic_name")}')
                logger.warning(msg)

            val = PublisherValueLttng(
                    pid=pub_df.get(i, 'pid'),
                    node_name=pub_df.get(i, 'node_name'),
                    topic_name=pub_df.get(i, 'topic_name'),
                    node_id=pub_df.get(i, 'node_id'),
                    callback_ids=None,
                    publisher_handle=pub_df.get(i, 'publisher_handle'),
                    publisher_id=pub_df.get(i, 'publisher_id'),
                    caret_rclcpp_version=caret_rclcpp_version,
                    tilde_publisher=tilde_publisher
                )
            pubs.add(pub_df.get(i, 'node_id'), val)

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
        pub_df = merge(
            pub_df,
            self._formatted.nodes_df,
            'node_handle',
            'node_handle',
            how='inner'
        )
        tilde_pub = self._formatted.tilde_publishers_df

        pub_df = merge(
            pub_df,
            tilde_pub,
            ['node_name', 'topic_name'],
            ['node_name', 'topic_name'],
            how='left')
        pubs_info = []

        for i in range(len(pub_df)):
            node_id_ = pub_df.sget(i, 'node_id')
            topic_name = pub_df.sget(i, 'topic_name')
            if node_id_ != node_id:
                continue
            if topic_name not in ['/tf', '/tf_static']:
                continue
            tilde_publisher = pub_df.iget(i, 'tilde_publisher')
            pid = pub_df.iget(i, 'pid')
            node_name = pub_df.sget(i, 'node_name')
            publisher_handle = pub_df.iget(i, 'publisher_handle')
            publisher_id = pub_df.sget(i, 'publisher_id')
            lib_caret_version = pub_df.sget(i, 'caret_rclcpp_version')

            assert pid is not None
            assert node_name is not None
            assert publisher_handle is not None
            assert publisher_id is not None

            pubs_info.append(
                PublisherValueLttng(
                    pid=pid,
                    node_name=node_name,
                    topic_name=topic_name,
                    node_id=node_id_,
                    callback_ids=None,
                    publisher_handle=publisher_handle,
                    publisher_id=publisher_id,
                    tilde_publisher=tilde_publisher,
                    caret_rclcpp_version=lib_caret_version
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
            'pid', 'callback_group_addr', 'callback_id', 'node_handle'
        ]
        for df in concate_target_dfs:
            df.columns.drop(set(df.column_names) - set(column_names))
            df.columns.reindex(column_names)
        concat_df = concate_target_dfs[0]
        for df in concate_target_dfs[1:]:
            concat_df.concat(df)

        df = concat_df.to_dataframe()
        concat_df = merge(
            concat_df,
            formatted.nodes_df,
            'node_handle',
            'node_handle',
            'inner'
        )
        concat_df = merge(
            concat_df,
            formatted.callback_groups_df,
            'callback_group_addr',
            'callback_group_addr',
            'inner'
        )

        for _, group_df in concat_df.groupby(['callback_group_addr']).items():
            node_name = group_df.get(0, 'node_name')
            callback_ids = tuple(group_df.get(i, 'callback_id') for i in range(len(group_df)))
            callback_ids = tuple(Util.filter_items(callbacks.is_user_defined, callback_ids))

            val = CallbackGroupValueLttng(
                    pid=group_df.get(0, 'pid'),
                    callback_group_type_name=group_df.get(0, 'group_type_name'),
                    node_name=node_name,
                    node_id=group_df.get(0, 'node_id'),
                    callback_ids=callback_ids,
                    callback_group_id=group_df.get(0, 'callback_group_id'),
                    callback_group_addr=group_df.get(0, 'callback_group_addr'),
                    executor_addr=group_df.get(0, 'executor_addr'),
                )

            cbgs.add(group_df.get(0, 'node_id'), val)

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

    @lru_cache
    def get_executors(self) -> List[ExecutorValueLttng]:
        """
        Get executors information.

        Returns
        -------
        List[ExecutorInfo]

        """
        exec_df = self._formatted.executor_df
        cbg_df = self._formatted.callback_groups_df
        exec_df = merge(
            exec_df,
            cbg_df,
            'executor_addr',
            'executor_addr',
            'inner'
        )
        execs: List[ExecutorValueLttng] = []

        for i, (_, group) in enumerate(exec_df.groupby(['executor_addr']).items()):
            executor_type_name = group.sget(0, 'executor_type_name')
            pid = group.iget(0, 'pid')
            assert pid is not None
            assert executor_type_name is not None

            cbg_ids = []
            for i in range(len(group)):
                cbg_id = group.sget(i, 'callback_group_id')
                assert cbg_id is not None
                cbg_ids.append(cbg_id)

            executor_id = f'executor_{i}'
            execs.append(
                ExecutorValueLttng(
                    pid,
                    executor_id,
                    executor_type_name,
                    tuple(cbg_ids))
            )

        return execs

    def get_publisher_qos(self, publisher: PublisherValueLttng) -> Qos:
        pub_df = self._formatted.publishers_df
        pub_df.filter_if(lambda r: r.get('publisher_handle')
                         == publisher.publisher_handle)

        if len(pub_df) == 0:
            raise InvalidArgumentError('No publisher matching the criteria was found.')
        if len(pub_df) > 1:
            logger.warning(
                'Multiple publishers matching your criteria were found.'
                'The value of the first publisher qos will be returned.')

        depth = pub_df.iget(0, 'depth')
        assert depth is not None
        return Qos(depth)

    def get_subscription_qos(self, callback: SubscriptionCallbackValueLttng) -> Qos:
        sub_df = self._formatted.subscription_callbacks_df
        sub_df.filter_if(lambda r: r.get('callback_object') == callback.callback_object)

        if len(sub_df) == 0:
            raise InvalidArgumentError('No subscription matching the criteria was found.')
        if len(sub_df) > 1:
            logger.warning(
                'Multiple publishers matching your criteria were found.'
                'The value of the first publisher qos will be returned.')

        depth = sub_df.iget(0, 'depth')
        assert depth is not None
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


class DataFrameFormatted:

    def __init__(self, data: Ros2DataModel):
        self._executor_df = self._build_executor_records(data)
        self._nodes_df = self._build_nodes_df(data)
        self._timer_callbacks_df = self._build_timer_callbacks_df(data)
        self._sub_callbacks_df = self._build_sub_callbacks_df(data)
        self._srv_callbacks_df = self._build_srv_callbacks_df(data)
        # self._clt_callbacks_df = self._build_clt_callbacks_df(data)
        self._cbg_df = self._build_cbg_df(data)
        self._pub_df = self._build_publisher_df(data)
        self._tilde_sub = self._build_tilde_subscription_df(data)
        self._tilde_pub = self._build_tilde_publisher_df(data)
        self._tilde_sub_id_to_sub = self._build_tilde_sub_id_df(data, self._tilde_sub)
        # self._timer_control = self._build_timer_control_df(data)
        self._tf_broadcasters = self._build_tf_broadcasters_df(data, self._pub_df)
        self._tf_frames = self._build_tf_frames_df(data)
        self._tf_buffers = self._build_tf_buffers_df(
            data,
            self._nodes_df.clone(),
            self._timer_callbacks_df.clone(),
            self._sub_callbacks_df.clone(),
            self._srv_callbacks_df.clone(),
        )
        self._broadcaster_frame_id_df = data.init_tf_broadcaster_frame_id_compact
        self._buffer_frame_id_df = data.init_tf_buffer_frame_id_compact
        self._ipm_df = self._build_ipm_df(data, self._pub_df, self._sub_callbacks_df)
        self. _ipc_buffers_df = self._build_ipc_buffer_df(
            data, self._sub_callbacks_df, self._nodes_df)

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
    def timer_callbacks_df(self) -> RecordsInterface:
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
        return self._timer_callbacks_df.clone()

    @property
    def subscription_callbacks_df(self) -> RecordsInterface:
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
        return self._sub_callbacks_df.clone()

    @property
    def nodes_df(self) -> RecordsInterface:
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
        return self._nodes_df.clone()

    @property
    def publishers_df(self) -> RecordsInterface:
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
        return self._pub_df.clone()

    @property
    def tf_broadcasters_df(self) -> RecordsInterface:
        return self._tf_broadcasters.clone()

    @property
    def tf_broadcaster_frame_id_df(self) -> RecordsInterface:
        return self._broadcaster_frame_id_df.clone()

    @property
    def tf_buffer_frame_id_df(self) -> RecordsInterface:
        return self._buffer_frame_id_df.clone()

    @property
    def ipc_buffers_df(self) -> RecordsInterface:
        return self._ipc_buffers_df.clone()

    @property
    def service_callbacks_df(self) -> RecordsInterface:
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
        return self._srv_callbacks_df.clone()

    @property
    def executor_df(self) -> RecordsInterface:
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
    def callback_groups_df(self) -> RecordsInterface:
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
        return self._cbg_df.clone()

    @property
    def tilde_publishers_df(self) -> RecordsInterface:
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
        return self._tilde_pub.clone()

    @property
    def tilde_subscriptions_df(self) -> RecordsInterface:
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
        return self._tilde_sub.clone()

    # @property
    # def timer_controls_df(self) -> RecordsInterface:
    #     return self._timer_control.clone()

    @property
    def tf_buffers_df(self) -> RecordsInterface:
        return self._tf_buffers.clone()

    @property
    def tf_frames_df(self) -> RecordsInterface:
        return self._tf_frames.clone()

    @property
    def ipm_df(self) -> RecordsInterface:
        return self._ipm_df.clone()

    @staticmethod
    def _build_ipm_df(
        data: Ros2DataModel,
        pub: pd.DataFrame,
        sub: pd.DataFrame,
    ):
        columns = [
            'pid',
            'ipm',
            'publisher_handle',
            'subscription_handle',
            'use_take_shared_method'
        ]

        df = data.construct_ipm.clone()

        df = merge(
            df,
            data.ipm_add_publisher.clone(),
            ['pid', 'ipm'],
            ['pid', 'ipm'],
            'inner'
        )

        df = merge(
            df,
            data.ipm_add_subscription.clone(),
            ['pid', 'ipm'],
            ['pid', 'ipm'],
            'inner'
        )

        df = merge(
            df,
            data.ipm_insert_sub_id_for_pub.clone(),
            ['pid', 'ipm', 'pub_id', 'sub_id'],
            ['pid', 'ipm', 'pub_id', 'sub_id'],
            'inner'
        )
        df.columns.drop(set(df.column_names) - set(columns))
        df.columns.reindex(columns)
        return df

    @staticmethod
    def _build_ipc_buffer_df(
        data: Ros2DataModel,
        sub_df: RecordsInterface,
        node_df: RecordsInterface,
    ):
        columns = [
            'pid', 'buffer', 'capacity', 'topic_name', 'node_name', 'node_id'
        ]

        records = data.construct_ring_buffer.clone()
        records.columns.rename({'timestamp': 'buffer_construct_timestamp'})
        records_ = data.rclcpp_subscription_init.clone()
        records_.columns.rename({'timestamp': 'rclcpp_sub_init_timestamp'})
        records = merge_sequencial(
            records,
            records_,
            left_stamp_key='buffer_construct_timestamp',
            right_stamp_key='rclcpp_sub_init_timestamp',
            how='inner',
            join_left_key=['pid', 'tid'],
            join_right_key=['pid', 'tid'],
        )
        records_ = sub_df.clone()
        records = merge(
            left_records=records,
            right_records=records_,
            # join_left_key=['subscription_handle', 'pid'],
            # join_right_key=['subscription_handle', 'pid'],
            # TODO: fix this bug
            join_left_key=['subscription_handle'],
            join_right_key=['subscription_handle'],
            how='inner'
        )
        records = merge(
            records,
            node_df,
            ['node_handle'],
            ['node_handle'],
            'inner'
        )

        records.columns.drop(set(records.column_names) - set(columns))
        records.columns.reindex(columns)
        records.drop_duplicates()
        return records

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
            'target_frame_id',
            'source_frame_id',
        ]

        df = data.construct_tf_buffer.clone()
        df = merge(
            df,
            data.init_bind_tf_buffer_core.clone(),
            ['tf_buffer_core', 'pid'],
            ['tf_buffer_core', 'pid'],
            'inner'
        )

        lookup_df = data.tf_buffer_lookup_transform.clone()
        df = merge(
            df,
            lookup_df,
            ['tf_buffer_core', 'pid'],
            ['tf_buffer_core', 'pid'],
            'inner'
        )

        cb_columns = ['callback_object', 'node_handle']
        timer_df.columns.drop(set(timer_df.column_names) - set(cb_columns))
        sub_df.columns.drop(set(sub_df.column_names) - set(cb_columns))
        srv_df.columns.drop(set(srv_df.column_names) - set(cb_columns))

        timer_df.concat(sub_df)
        timer_df.concat(srv_df)

        df = merge(
            df,
            timer_df,
            CN.CALLBACK,
            CN.CALLBACK_OBJECT,
            how='left'
        )

        df = merge(
            df,
            node_df,
            CN.NODE_HANDLE,
            CN.NODE_HANDLE,
            how='left'
        )
        df.columns.rename(
            {'node_id': 'listener_node_id', 'node_handle': 'listener_node_handle'},
        )

        df = merge(
            df,
            data.construct_node_hook.clone(),
            ['pid', 'clock'],
            ['pid', 'clock'],
            how='left'
        )

        df = merge(
            df,
            node_df,
            ['node_handle'],
            ['node_handle'],
            how='left'
        )
        # df.dropna(inplace=True)

        df.columns.rename(
            {'node_id': 'lookup_node_id', 'node_handle': 'lookup_node_handle'}
        )

        df.columns.drop(set(df.column_names) - set(columns))
        df.columns.reindex(columns)
        df.drop_duplicates()
        return df

    @staticmethod
    def _build_publisher_df(
        data: Ros2DataModel,
    ) -> pd.DataFrame:
        columns = [
            'pid',
            'publisher_id',
            'publisher_handle',
            'node_handle',
            'topic_name',
            'depth',
            'caret_rclcpp_version',
        ]
        df = data.rcl_publisher_init.clone()
        rclcpp_pub_inits = data.rclcpp_publisher_init.clone()
        df = merge(
            df,
            rclcpp_pub_inits,
            'publisher_handle',
            'publisher_handle',
            how='outer'
        )

        def to_publisher_id(i: int, row: Dict):
            return f'publisher_{i}'

        DataFrameFormatted._add_column(df, 'publisher_id', to_publisher_id)
        df.columns.drop(set(df.column_names) - set(columns))
        df.columns.reindex(columns)

        return df

    @staticmethod
    def _build_tf_broadcasters_df(
        data: Ros2DataModel,
        pub_df: pd.DataFrame
    ) -> pd.DataFrame:
        columns = [
            'pid',
            'transform_broadcaster',
            'publisher_id',
            'publisher_handle',
            'node_handle',
            'topic_name',
            'depth',
            'frame_id',
            'child_frame_id'
        ]
        df = data.transform_broadcaster.clone()
        tf_frames = data.transform_broadcaster_frames.clone()
        df = merge(
            df,
            tf_frames,
            'transform_broadcaster',
            'transform_broadcaster',
            how='outer'
        )

        df = merge(
            df,
            pub_df,
            'publisher_handle',
            'publisher_handle',
            how='left')

        df.columns.drop(set(df.column_names) - set(columns))
        df.columns.reindex(columns)
        return df

    @staticmethod
    def _build_tf_frames_df(
        data: Ros2DataModel,
    ) -> pd.DataFrame:
        columns = [
            'frame_id',
            'child_frame_id'
        ]
        br_df = data.transform_broadcaster_frames.clone()
        buf_df = data.tf_buffer_set_transform.clone()

        br_df.columns.drop(set(br_df.column_names) - set(columns))
        buf_df.columns.drop(set(buf_df.column_names) - set(columns))

        br_df.concat(buf_df)
        br_df.drop_duplicates()

        return br_df

    @staticmethod
    def _build_timer_control_df(
        data: Ros2DataModel,
    ) -> pd.DataFrame:
        columns = ['timestamp', 'timer_handle', 'type', 'params']
        df = data.rcl_timer_init.clone()

        record_list: List[RecordInterface] = []
        mapper = ColumnMapper()
        params_mapper = ColumnMapper()
        for i, row in enumerate(df):
            mapper.add(i, 'init')
            params_mapper.add(i, {'period': row.get('period')})
            record_list.append(
                RecordFactory.create_instance(
                    {
                        'type': i,
                        'params': i,
                    }
                )
            )

        records = RecordsFactory.create_instance(
            record_list,
            [
                ColumnValue('timestamp', [ColumnAttribute.SYSTEM_TIME]),
                ColumnValue('timer_handle'),
                ColumnValue('type', mapper=mapper),
                ColumnValue('params', mapper=params_mapper),
            ]
        )
        records.columns.reindex(columns)
        return records

    @staticmethod
    def _build_executor_records(
        data: Ros2DataModel,
    ) -> pd.DataFrame:
        columns = ['pid', 'executor_id', 'executor_addr', 'executor_type_name']

        records = data.construct_executor.clone()

        records_ = data.construct_static_executor.clone()
        records_.columns.drop(['entities_collector_addr'])
        records.concat(records_)

        def to_executor_id(i: int, d) -> str:
            return f'executor_{i}'

        DataFrameFormatted._add_column(records, 'executor_id', to_executor_id)

        records.columns.drop(set(records.column_names) - set(columns))
        records.columns.reindex(columns)

        # data.callback_groups returns duplicate results that differ only in timestamp.
        # Remove duplicates to make it unique.
        records.drop_duplicates()

        return records

    @staticmethod
    def _build_cbg_df(
        data: Ros2DataModel,
    ) -> pd.DataFrame:
        columns = ['callback_group_id', 'callback_group_addr', 'group_type_name', 'executor_addr']
        df = data.add_callback_group.clone()

        df.columns.drop(['timestamp'])
        # data.callback_groups returns duplicate results that differ only in timestamp.
        # Remove duplicates to make it unique.
        df.drop_duplicates()

        df_static = data.add_callback_group_static_executor.clone()
        df_static_exec = data.construct_static_executor.clone()

        if len(df_static) > 0 and len(df_static_exec) > 0:
            df_static = merge(
                df_static,
                df_static_exec,
                'entities_collector_addr',
                'entities_collector_addr',
                'inner'
            )
            raise NotImplementedError('TODO support static executor')
            # columns_ = columns[1:]  # ignore callback_group_id
            # df = concat(columns_, [df, df_static])

        def to_callback_group_id(i: int, row: Dict) -> str:
            return f'callback_group_{i}'

        DataFrameFormatted._add_column(df, 'callback_group_id', to_callback_group_id)

        executor_duplicated_indexes = []
        for _, group in df.groupby(['callback_group_addr']).items():
            if len(group) >= 2:
                msg = ('Multiple executors using the same callback group were detected.'
                       'The last executor will be used. ')
                exec_addr = group.get_column_series('executor_addr')
                msg += f'executor address: {exec_addr}'
                logger.warn(msg)
                executor_duplicated_indexes += list(group.index)[:-1]

        if len(executor_duplicated_indexes) >= 1:
            df.drop(index=executor_duplicated_indexes, inplace=True)

        df.columns.drop(set(df.column_names) - set(columns))
        df.columns.reindex(columns)
        return df

    @staticmethod
    def _build_timer_callbacks_df(
        data: Ros2DataModel,
    ) -> pd.DataFrame:
        columns = [
            'pid', 'callback_id', 'callback_object', 'node_handle',
            'timer_handle', 'callback_group_addr', 'period_ns', 'symbol',
        ]

        def callback_id(i: int, row: Dict) -> str:
            return f'timer_callback_{i}'

        records = data.rcl_timer_init.clone()

        timer_node_links_records = data.rclcpp_timer_link_node.clone()
        records = merge(
            left_records=records,
            right_records=timer_node_links_records,
            join_left_key=['pid', 'timer_handle'],
            join_right_key=['pid', 'timer_handle'],
            how='inner'
        )

        callback_objects_records = data.rclcpp_timer_callback_added.clone()
        records = merge(
            records,
            callback_objects_records,
            ['pid', 'timer_handle'],
            ['pid', 'timer_handle'],
            how='inner'
        )

        symbols_records = data.rclcpp_callback_register.clone()
        records = merge(
            records,
            symbols_records,
            ['pid', 'callback_object'],
            ['pid', 'callback_object'],
            how='inner'
        )

        cbg = data.callback_group_add_timer.clone()
        records = merge(
            records,
            cbg,
            ['pid', 'timer_handle'],
            ['pid', 'timer_handle'],
            how='inner'
        )

        DataFrameFormatted._add_column(
            records, 'callback_id', callback_id,
            value_offset=10**5)

        records.columns.rename({'period': 'period_ns'})
        records.columns.drop(set(records.column_names) - set(columns))
        records.columns.reindex(columns)

        return records

    @staticmethod
    def _build_sub_callbacks_df(
        data: Ros2DataModel,
    ) -> pd.DataFrame:
        columns = [
            'pid', 'callback_id', 'callback_object', 'callback_object_intra', 'node_handle',
            'subscription_handle', 'callback_group_addr', 'topic_name', 'symbol', 'depth'
        ]

        def callback_id(i: int, row: Dict) -> str:
            return f'subscription_callback_{i}'

        records = data.rcl_subscription_init.clone()

        callback_objects_df = DataFrameFormatted._format_subscription_callback_object(data)
        records = merge(
            records,
            callback_objects_df,
            ['pid', 'subscription_handle'],
            ['pid', 'subscription_handle'],
            how='inner'
        )

        symbols_df = data.rclcpp_callback_register.clone()
        records = merge(
            records,
            symbols_df,
            ['pid', 'callback_object'],
            ['pid', 'callback_object'],
            how='inner'
        )

        cbg = data.callback_group_add_subscription.clone()
        records = merge(
            records,
            cbg,
            ['pid', 'subscription_handle'],
            ['pid', 'subscription_handle'],
            how='inner'
        )

        DataFrameFormatted._add_column(
            records, 'callback_id', callback_id,
            value_offset=10**5*2)
        records.columns.drop(set(records.column_names) - set(columns))
        records.columns.reindex(columns)

        return records

    @staticmethod
    def _build_srv_callbacks_df(
        data: Ros2DataModel,
    ) -> pd.DataFrame:
        columns = [
            'pid', 'callback_id', 'callback_object', 'node_handle',
            'service_handle', 'callback_group_addr', 'service_name', 'symbol'
        ]

        def callback_id(i: int, row: Dict) -> str:
            return f'service_callback_{i}'

        records = data.rcl_service_init.clone()

        callback_objects_records = data.rclcpp_service_callback_added.clone()
        records = merge(
            records,
            callback_objects_records,
            ['pid', 'service_handle'],
            ['pid', 'service_handle'],
            'inner'
        )

        symbols_records = data.rclcpp_callback_register.clone()
        records = merge(
            records,
            symbols_records,
            ['pid', 'callback_object'],
            ['pid', 'callback_object'],
            'inner'
        )

        cbg = data.callback_group_add_service.clone()
        records = merge(
            records,
            cbg,
            ['pid', 'service_handle'],
            ['pid', 'service_handle'],
            'inner'
        )

        DataFrameFormatted._add_column(
            records, 'callback_id', callback_id,
            value_offset=10**5*3)

        records.columns.drop(set(records.column_names) - set(columns))
        records.columns.reindex

        return records

    @staticmethod
    def _build_clt_callbacks_df(
        data: Ros2DataModel,
    ) -> pd.DataFrame:
        raise NotImplementedError('')

    @staticmethod
    def _build_tilde_subscription_df(
        data: Ros2DataModel,
    ) -> pd.DataFrame:
        columns = ['tilde_subscription', 'node_name', 'topic_name']

        df = data.tilde_subscription_init.clone()
        df.columns.drop(set(df.column_names) - set(columns))
        df.columns.reindex(columns)

        return df

    @staticmethod
    def _build_tilde_publisher_df(
        data: Ros2DataModel,
    ) -> pd.DataFrame:
        columns = ['tilde_publisher', 'node_name', 'topic_name']

        df = data.tilde_publisher_init.clone()

        df.columns.drop(set(df.column_names) - set(columns))
        df.columns.reindex(columns)
        return df

    @staticmethod
    def _build_tilde_sub_id_df(
        data: Ros2DataModel,
        sub_df: pd.DataFrame
    ) -> pd.DataFrame:
        columns = ['tilde_subscription_id', 'tilde_subscription', 'node_name', 'topic_name']
        df = data.tilde_subscribe_added.clone()
        df = merge(
            df,
            sub_df,
            ['node_name', 'topic_name'],
            ['node_name', 'topic_name'],
            how='left')
        df.columns.drop(set(df.column_names) - set(columns))
        df.columns.reindex(columns)
        return df

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

        df = data.subscriptions

        callback_objects_df = DataFrameFormatted._format_subscription_callback_object(data)
        df = merge(
            df,
            callback_objects_df,
            'subscription_handle',
            'subscription_handle',
            'inner'
        )

        symbols_df = data.rclcpp_callback_register
        df = merge(
            df,
            symbols_df,
            'callback_object',
            'callback_object',
            'inner'
        )

        cbg = data.callback_group_add_subscription
        df = merge(
            df,
            cbg,
            'subscription_handle',
            'subscription_handle',
            'inner'
        )

        df = DataFrameFormatted._add_column(
            df, 'callback_id', callback_id,
            value_offset=10**5*4)
        return df[columns]

    @staticmethod
    def _is_ignored_subscription(
        data: Ros2DataModel,
        pid: int,
        subscription_handle: int,
    ) -> bool:
        sub_df = data.rcl_subscription_init.clone()
        sub_df.filter_if(
            lambda row: row.get('pid') == pid and
            row.get('subscription_handle') == subscription_handle
        )
        sub_df = merge(
            sub_df,
            data.rcl_node_init.clone(),
            'node_handle',
            'node_handle',
            'inner'
        )

        ns = sub_df.data[0].get('namespace')
        name = sub_df.data[0].get('name')
        column = sub_df.columns.get('topic_name')
        topic_name = column.mapper.get(sub_df.data[0].get('topic_name'))

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
            - callback_object_intra

        Raises
        ------
        InvalidArgumentError

        """
        callback_objects_df = data.rclcpp_subscription_callback_added.clone()
        sub_df = data.rclcpp_subscription_init.clone()
        sub_df = merge(
            sub_df,
            callback_objects_df,
            ['pid', 'subscription'],
            ['pid', 'subscription'],
            'inner'
        )

        dicts: List[RecordInterface] = []
        for key, group in sub_df.groupby(['pid', 'subscription_handle']).items():
            pid, subscription_handle = key[0], key[1]

            if DataFrameFormatted._is_ignored_subscription(data, pid, subscription_handle):
                continue

            record = {
                'pid': pid,
                'subscription_handle': subscription_handle,
            }
            if len(group) == 1:
                record['callback_object'] = group.get(0, 'callback_object')
            elif len(group) == 2:
                record['callback_object'] = group.get(0, 'callback_object')
                record['callback_object_intra'] = group.get(1, 'callback_object')
            else:
                cb_objs = group['callback_object'].values
                logger.warning(
                    'More than three callbacks are registered in one subscription_handle. '
                    'Skip loading callback info. The following callbacks cannot be measured.'
                    f'pid = {pid}, '
                    f'subscription_handle = {subscription_handle}, '
                    f'callback_objects = {cb_objs}')
            dicts.append(RecordFactory.create_instance(record))

        records = RecordsFactory.create_instance(
            dicts,
            [
                ColumnValue('pid'),
                ColumnValue('subscription_handle'),
                ColumnValue('callback_object'),
                ColumnValue('callback_object_intra'),
            ]
        )

        return records

    @staticmethod
    def _add_column(
        records: RecordsInterface,
        column_name: str,
        cell_rule: Callable[[int, Dict[str, Union[str, int]]], str],
        value_offset=0
    ) -> None:
        # Add offset to column mapper to avoid duplicate values
        values = []
        mapper = ColumnMapper()
        for i, datum in enumerate(records.data):
            d: Dict[str, Union[str, int]] = {}
            for j, column in enumerate(records.column_names):
                column_mapper = records.columns[j].mapper
                if column not in datum.columns:
                    continue
                if column_mapper is not None:
                    d[column] = column_mapper.get(datum.get(column))
                else:
                    d[column] = datum.get(column)
            mapper.add(i+value_offset, cell_rule(i, d))

            values.append(i+value_offset)
        column_value = ColumnValue(column_name, mapper=mapper)
        records.append_column(column_value, values)
        return None

    @staticmethod
    def _build_nodes_df(
        data: Ros2DataModel
    ) -> RecordsInterface:
        columns = ['pid', 'node_id', 'node_handle', 'node_name', 'lib_caret_version']

        node_records = data.rcl_node_init.clone()

        def ns_and_node_name(i: int, row: Dict) -> str:
            ns: str = row['namespace']
            name: str = row['name']

            if ns[-1] == '/':
                return ns + name
            else:
                return ns + '/' + name

        DataFrameFormatted._add_column(node_records, 'node_name', ns_and_node_name)

        def to_node_id(i: int, row: Dict) -> str:
            return f'node_{i}'

        DataFrameFormatted._add_column(node_records, 'node_id', to_node_id)

        rcl_caret_inits = data.rcl_init_caret.clone()
        node_records = merge(
            node_records,
            rcl_caret_inits,
            'pid',
            'pid',
            how='outer'
        )

        node_records.columns.drop(set(node_records.column_names) - set(columns))
        node_records.columns.reindex(columns)

        return node_records
