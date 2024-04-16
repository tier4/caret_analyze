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

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from functools import cached_property, lru_cache
from logging import getLogger, WARN

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
                            TimerInit,
                            TimerControl)
from ..trace_point_data import TracePointData, TracePointIntermediateData
from ...common import Util
from ...exceptions import InvalidArgumentError
from ...value_objects import ExecutorValue, NodeValue, Qos, TimerValue


logger = getLogger(__name__)


class LttngInfo:

    def __init__(self, data: Ros2DataModel):
        self._formatted = DataFrameFormatted(data)

        # TODO(hsgwa): check rmw_impl for each process.
        self._rmw_implementation = data.rmw_impl.iat(0, 0) if len(data.rmw_impl) > 0 else ''
        self._distribution = self._get_distribution(data)

        self._id_to_topic: dict[str, str] = {}
        self._id_to_service: dict[str, str] = {}

    def _get_timer_cbs_without_pub(self, node_id: str) -> list[TimerCallbackValueLttng]:
        timer_cb_cache_without_pub = self._load_timer_cbs_without_pub()

        if node_id not in timer_cb_cache_without_pub:
            return []

        return timer_cb_cache_without_pub[node_id]

    def _get_sub_cbs_without_pub(self, node_id: str) -> list[SubscriptionCallbackValueLttng]:
        sub_cb_cache_without_pub = self._load_sub_cbs_without_pub()

        if node_id not in sub_cb_cache_without_pub:
            return []

        return sub_cb_cache_without_pub[node_id]

    def _get_srv_cbs_without_pub(self, node_id: str) -> list[ServiceCallbackValueLttng]:
        srv_cb_cache_without_pub = self._load_srv_cbs_without_pub()

        if node_id not in srv_cb_cache_without_pub:
            return []

        return srv_cb_cache_without_pub[node_id]

    def get_rmw_impl(self) -> str:
        """
        Get rmw implementation.

        Returns
        -------
        str
            rmw_implementation

        """
        return self._rmw_implementation

    def _get_distribution(self, data: Ros2DataModel) -> str:
        caret_init_df = data.caret_init.df
        distributions = list(caret_init_df['distribution'].unique())
        if len(distributions) > 1:
            logger.info('Multiple ros distributions are found.')

        if len(distributions) == 0:
            return 'NOTFOUND'

        return distributions[0]

    def get_distribution(self) -> str:
        """
        Get ROS Distribution.

        Returns
        -------
        str
            ROS Distribution

        """
        return self._distribution

    @lru_cache
    def _load_timer_cbs_without_pub(self) -> dict[str, list[TimerCallbackValueLttng]]:
        timer_cbs_info: dict[str, list[TimerCallbackValueLttng]] = {}

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

    @lru_cache
    def _get_timer_callbacks(self, node: NodeValue) -> Sequence[TimerCallbackValueLttng]:
        node_id = node.node_id
        assert node_id is not None

        timer_cbs = self._get_timer_cbs_without_pub(node_id)

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
        if node.node_id is None:
            return Util.flatten([
                self._get_timer_callbacks(node)
                for node
                in self._get_nodes(node.node_name)
            ])

        node_lttng = NodeValueLttng(node.node_name, node.node_id)
        return self._get_timer_callbacks(node_lttng)

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

    @lru_cache
    def _load_sub_cbs_without_pub(
        self
    ) -> dict[str, list[SubscriptionCallbackValueLttng]]:
        sub_cbs_info: dict[str, list[SubscriptionCallbackValueLttng]] = {}

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

    @lru_cache
    def _get_subscription_callback_values(
        self,
        node: NodeValue
    ) -> list[SubscriptionCallbackValueLttng]:
        node_id = node.node_id
        assert node_id is not None

        sub_cbs_info: list[SubscriptionCallbackValueLttng]
        sub_cbs_info = self._get_sub_cbs_without_pub(node_id)
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
        if node.node_id is None:
            return Util.flatten([
                self._get_subscription_callback_values(node)
                for node
                in self._get_nodes(node.node_name)
            ])

        node_lttng = NodeValueLttng(node.node_name, node.node_id)
        return self._get_subscription_callback_values(node_lttng)

    @property
    def tilde_sub_id_map(self) -> dict[int, int]:
        return self._formatted.tilde_sub_id_map

    @lru_cache
    def _load_srv_cbs_without_pub(
        self
    ) -> dict[str, list[ServiceCallbackValueLttng]]:
        srv_cbs_info: dict[str, list[ServiceCallbackValueLttng]] = {}

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

    @lru_cache
    def _get_service_callback_values(
        self,
        node: NodeValue
    ) -> list[ServiceCallbackValueLttng]:
        node_id = node.node_id
        assert node_id is not None

        srv_cbs_info: list[ServiceCallbackValueLttng]
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
        if node.node_id is None:
            return Util.flatten([
                self._get_service_callback_values(node)
                for node
                in self._get_nodes(node.node_name)
            ])

        node_lttng = NodeValueLttng(node.node_name, node.node_id)
        return self._get_service_callback_values(node_lttng)

    @lru_cache
    def _get_publishers(self, node: NodeValueLttng) -> list[PublisherValueLttng]:
        node_id = node.node_id
        return self._get_publishers_without_cb_bind(node_id)

    def get_publishers(self, node: NodeValue) -> list[PublisherValueLttng]:
        """
        Get publishers information.

        Parameters
        ----------
        node: NodeValue
            target node.

        Returns
        -------
        list[PublisherInfo]

        """
        if node.node_id is None:
            return Util.flatten([
                self._get_publishers(node)
                for node
                in self._get_nodes(node.node_name)
            ])

        node_lttng = NodeValueLttng(node.node_name, node.node_id)
        return self._get_publishers(node_lttng)

    def _get_nodes(
        self,
        node_name: str
    ) -> Sequence[NodeValueLttng]:
        return Util.filter_items(lambda x: x.node_name == node_name, self.get_nodes())

    def _get_publishers_without_cb_bind(self, node_id: str) -> list[PublisherValueLttng]:
        """
        Get publishers information.

        Parameters
        ----------
        node_id : str
            node ID

        Returns
        -------
        list[PublisherInfo]

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
                    tilde_publisher=tilde_publisher,
                    construction_order=row['construction_order']
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

    @lru_cache
    def _get_callback_groups(
        self,
        node_id: str
    ) -> list[CallbackGroupValueLttng]:
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

            callback_groups_values: list[CallbackGroupValueLttng] = []
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
        list[CallbackGroupInfo]

        """
        if node.node_id is None:
            return Util.flatten([
                self._get_callback_groups(node.node_id)
                for node
                in self._get_nodes(node.node_name)
            ])

        node_lttng = NodeValueLttng(node.node_name, node.node_id)
        return self._get_callback_groups(node_lttng.node_id)

    def get_executors(self) -> list[ExecutorValue]:
        """
        Get executors information.

        Returns
        -------
        list[ExecutorInfo]

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
                        construction_order=callback.construction_order
                    )
                )

            return timers
        except ValueError:
            return []

    def get_timer_controls(self) -> Sequence[TimerControl]:
        timer_controls = self._formatted.timer_controls.clone()
        controls: list[TimerControl] = []
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
    def tilde_sub_id_map(self) -> dict[int, int]:
        d: dict[int, int] = {}
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
        columns = (
            ['publisher_id', 'publisher_handle', 'node_handle',
             'topic_name', 'depth', 'construction_order']
        )
        publishers = data.publishers.clone()
        publishers.reset_index()

        DataFrameFormatted._add_construction_order_publisher_or_subscription(
            publishers, 'construction_order', 'timestamp', 'node_handle', 'topic_name')

        def to_publisher_id(row: pd.Series):
            publisher_handle = row['publisher_handle']
            return f'publisher_{publisher_handle}'

        publishers.add_column('publisher_id', to_publisher_id)
        publishers.set_columns(columns)
        publishers.drop_duplicate()

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

        callback_groups.drop_duplicate()
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

        merge_drop_columns = ['tid']
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

        # In the case of runtime recording, there are cases where duplicates are recorded.
        # If duplicates are left, the instance cannot be uniquely identified and a warning will
        # be issued, so they should be deleted.
        timers.drop_duplicate()

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
        subscriptions.drop_duplicate()

        return subscriptions

    KeyType = tuple[int | str, int | str, str | int]

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
        order: dict[DataFrameFormatted.KeyType, int] = defaultdict(int)

        def construct_order(row: pd.Series) -> int:
            node = row[node_handle_column]
            callback_param = row[callback_parameter_column]
            symbol = row[symbol_column]
            key: DataFrameFormatted.KeyType = node, callback_param, symbol
            order_ = int(order[key])
            order[key] += 1
            return order_

        data.add_column(column_name, construct_order)

    KeyTypePubSub = tuple[int | str, str | int]

    @staticmethod
    def _add_construction_order_publisher_or_subscription(
        data: TracePointData,
        column_name: str,
        timestamp_column: str,
        node_handle_column: str,
        topic_name: str
    ) -> None:

        data.sort(timestamp_column)
        order: dict[DataFrameFormatted.KeyTypePubSub, int] = defaultdict(int)

        def construct_order(row: pd.Series) -> int:
            node = row[node_handle_column]
            key: DataFrameFormatted.KeyTypePubSub = node, row[topic_name]
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
        services.drop_duplicate()

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
        tilde_subscriptions.drop_duplicate()
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
        tilde_publishers.drop_duplicate()
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
        tilde_subscribe_added.drop_duplicate()
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
        subscriptions.drop_duplicate()

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

        trace_data = ret_data.get_finalized()
        trace_data.drop_duplicate()
        return trace_data

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
        node.drop_duplicate()

        return node


def merge(
    left_data: TracePointData,
    right_data: TracePointData,
    on,
    how: str | None = None,
    merge_drop_columns: list[str] | None = None
):
    how = how or 'inner'
    merge_drop_columns = merge_drop_columns or ['timestamp', 'tid', 'rmw_handle']
    left_data.merge(right_data, on, how=how, drop_columns=merge_drop_columns)
