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


import pytest

from trace_analysis.record.lttng import Lttng
from trace_analysis.callback import SubscriptionCallback

listener_callback = SubscriptionCallback(
    None,
    "/listener",
    SubscriptionCallback.to_callback_name(0),
    "demo_nodes_cpp::Listener::Listener(rclcpp::NodeOptionsconst&)::{lambda(std::shared_ptr<std_msgs::msg::String>)#1}",  # noqa: 501
    "/chatter",
)

pipe2_callback = SubscriptionCallback(
    None,
    "/pipe2",
    SubscriptionCallback.to_callback_name(0),
    "IncrementerPipe::IncrementerPipe(std::__cxx11::basic_string<char,std::char_traits<char>,std::allocator<char>>const&,std::__cxx11::basic_string<char,std::char_traits<char>,std::allocator<char>>const&,std::__cxx11::basic_string<char,std::char_traits<char>,std::allocator<char>>const&)::{lambda(std::unique_ptr<std_msgs::msg::Int32>)#1}",  # noqa: 501
    "/topic2",
)


class TestLttng:
    @pytest.mark.parametrize(
        "path, node_names_expect",
        [
            ("sample/lttng_samples/talker_listener", ["/talker", "/listener"]),
            ("sample/lttng_samples/cyclic_pipeline_intra_process", ["/pipe1", "/pipe2"]),
            (
                "sample/lttng_samples/multi_talker_listener",
                ["/ns1/talker", "/ns1/listener", "/ns2/talker", "/ns2/listener"],
            ),
            (
                "sample/lttng_samples/end_to_end_sample",
                [
                    "/actuator_dummy_node",
                    "/filter_node",
                    "/message_driven_node",
                    "/timer_driven_node",
                    "/sensor_dummy_node",
                    "/drive_node",
                ],
            ),
        ],
    )
    def test_get_node_names(self, path, node_names_expect):
        lttng = Lttng(path)
        node_names = lttng.get_node_names()
        assert set(node_names) == set(node_names_expect)

    @pytest.mark.parametrize(
        "path, node_name, topic_name, pub_attrs_len",
        [
            ("sample/lttng_samples/talker_listener", None, None, 5),
            ("sample/lttng_samples/talker_listener", "/talker", None, 3),
            ("sample/lttng_samples/talker_listener", "/talker", "/chatter", 1),
            ("sample/lttng_samples/cyclic_pipeline_intra_process", None, None, 6),
            ("sample/lttng_samples/cyclic_pipeline_intra_process", "/pipe1", None, 3),
            ("sample/lttng_samples/cyclic_pipeline_intra_process", "/pipe1", "/topic2", 1),
        ],
    )
    def test_get_publisher_attrs(self, path, node_name, topic_name, pub_attrs_len):
        lttng = Lttng(path)
        pub_attrs = lttng.get_publishers(node_name, topic_name)
        assert len(pub_attrs) == pub_attrs_len

    @pytest.mark.parametrize(
        "path, node_name, topic_name, attrs_len",
        [
            ("sample/lttng_samples/talker_listener", None, None, 3),
            ("sample/lttng_samples/talker_listener", "/listener", None, 2),
            ("sample/lttng_samples/talker_listener", "/listener", "/chatter", 1),
            ("sample/lttng_samples/cyclic_pipeline_intra_process", None, None, 4),
            ("sample/lttng_samples/cyclic_pipeline_intra_process", "/pipe1", None, 2),
            ("sample/lttng_samples/cyclic_pipeline_intra_process", "/pipe1", "/topic1", 1),
        ],
    )
    def test_get_subscription_callback_attrs_with_empty_publish(
        self, path, node_name, topic_name, attrs_len
    ):
        lttng = Lttng(path)
        attrs = lttng.get_subscription_callbacks(node_name, topic_name)
        assert len(attrs) == attrs_len

    @pytest.mark.parametrize(
        "path, node_name, period_ns, cbs_len",
        [
            ("sample/lttng_samples/talker_listener", None, None, 1),
            ("sample/lttng_samples/talker_listener", "/talker", None, 1),
            ("sample/lttng_samples/talker_listener", "/talker", 1000000000, 1),
            ("sample/lttng_samples/cyclic_pipeline_intra_process", None, None, 0),
            ("sample/lttng_samples/multi_talker_listener", None, None, 2),
            ("sample/lttng_samples/end_to_end_sample", None, None, 3),
        ],
    )
    def test_get_timer_callback_attrs_with_empty_publish(
        self, path, node_name, period_ns, cbs_len
    ):
        lttng = Lttng(path)
        callbacks = lttng.get_timer_callbacks(node_name, period_ns)
        assert len(callbacks) == cbs_len

    @pytest.mark.parametrize(
        "path, attr, records_len",
        [
            ("sample/lttng_samples/talker_listener", listener_callback, 5),
            ("sample/lttng_samples/cyclic_pipeline_intra_process", pipe2_callback, 5),
        ],
    )
    def test_compose_callback_records(self, path, attr, records_len):
        lttng = Lttng(path)
        records = lttng.compose_callback_records(attr)
        assert len(records) == records_len

    @pytest.mark.parametrize(
        "path, sub_node_name, pub_node_name, topic_name, records_len",
        [
            ("sample/lttng_samples/talker_listener", "/listener", "/talker", "/chatter", 5),
            (
                "sample/lttng_samples/end_to_end_sample",
                "/filter_node",
                "/sensor_dummy_node",
                "/topic1",
                201,
            ),
        ],
    )
    def test_compose_inter_process_communication_records(
        self,
        path: str,
        sub_node_name: str,
        pub_node_name: str,
        topic_name: str,
        records_len: int,
    ):
        lttng = Lttng(path)

        sub_cb = lttng.get_subscription_callbacks(node_name=sub_node_name, topic_name=topic_name)[
            0
        ]
        pub_cb = lttng.get_timer_callbacks(node_name=pub_node_name)[0]
        records = lttng.compose_inter_process_communication_records(sub_cb, pub_cb)
        assert len(records) == records_len

    @pytest.mark.parametrize(
        "path, pub_node_name, sub_node_name, records_len",
        [
            ("sample/lttng_samples/talker_listener", "/talker", "/listener", 0),
            ("sample/lttng_samples/cyclic_pipeline_intra_process", "/pipe1", "/pipe2", 5),
        ],
    )
    def test_compose_intra_process_communication_records(
        self, path, pub_node_name, sub_node_name, records_len
    ):
        def get_cb(node_name: str):
            cb = lttng.get_timer_callbacks(node_name)
            cb += lttng.get_subscription_callbacks(node_name)
            return cb[0]

        lttng = Lttng(path)
        pub = get_cb(pub_node_name)
        sub = get_cb(sub_node_name)
        records = lttng.compose_intra_process_communication_records(sub, pub)
        assert len(records) == records_len

    @pytest.mark.parametrize(
        "path, attr, records_len",
        [
            ("sample/lttng_samples/end_to_end_sample", listener_callback, 173),
        ],
    )
    def test_compose_variable_passing_records(self, path, attr, records_len):
        lttng = Lttng(path)

        callback_write = SubscriptionCallback(
            None,
            "/message_driven_node",
            SubscriptionCallback.to_callback_name(0),
            "SubDependencyNode::SubDependencyNode(std::__cxx11::basic_string<char,std::char_traits<char>,std::allocator<char>>,std::__cxx11::basic_string<char,std::char_traits<char>,std::allocator<char>>,std::__cxx11::basic_string<char,std::char_traits<char>,std::allocator<char>>,std::__cxx11::basic_string<char,std::char_traits<char>,std::allocator<char>>)::{lambda(std::unique_ptr<sensor_msgs::msg::Image>)#1}",  # noqa: 501
            "/topic2",
        )

        callback_read = SubscriptionCallback(
            None,
            "/message_driven_node",
            SubscriptionCallback.to_callback_name(1),
            "SubDependencyNode::SubDependencyNode(std::__cxx11::basic_string<char,std::char_traits<char>,std::allocator<char>>,std::__cxx11::basic_string<char,std::char_traits<char>,std::allocator<char>>,std::__cxx11::basic_string<char,std::char_traits<char>,std::allocator<char>>,std::__cxx11::basic_string<char,std::char_traits<char>,std::allocator<char>>)::{lambda(std::unique_ptr<sensor_msgs::msg::Image>)#2}",  # noqa: 501
            "/drive",
        )

        records = lttng.compose_variable_passing_records(callback_write, callback_read)
        assert len(records) == records_len
