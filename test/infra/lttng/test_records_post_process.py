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


from caret_analyze.infra.lttng.records_post_process import (
    unify_callback_records,
    unify_publish_records,
    unify_tf_lookup_records,
)
from caret_analyze.infra.lttng.ros2_tracing.data_model import Ros2DataModel


class TestCallbackRecordsPostProcess:

    def test_unify_inter_callback_records(self):
        data = Ros2DataModel()
        data.add_callback_start(
            pid=1,
            tid=2,
            timestamp=12,
            callback=4,
            is_intra_process=False,
        )
        data.add_callback_end(
            pid=1,
            tid=2,
            timestamp=13,
            callback=4,
        )
        data.add_dispatch_subscription_callback(
            pid=1,
            tid=2,
            timestamp=11,
            callback_object=4,
            message=5,
            source_timestamp=7,
            message_timestamp=8,
        )
        unify_callback_records(data)

        data_expect = Ros2DataModel()
        data_expect.add_inter_callback_duration(
            pid=1,
            tid=2,
            callback=4,
            callback_start_timestamp_raw=12,
            callback_end_timestamp_raw=13,
            callback_end_timestamp=13,
            source_timestamp=7,
            message_timestamp=8,
        )

        assert data.inter_callback_duration.equals(data_expect.inter_callback_duration)

    def test_unify_intra_callback_records(self):
        data = Ros2DataModel()
        data.add_callback_start(
            pid=1,
            tid=2,
            timestamp=12,
            callback=3,
            is_intra_process=True,
        )
        data.add_callback_end(
            pid=1,
            tid=2,
            timestamp=13,
            callback=3,
        )
        data.add_dispatch_intra_process_subscription_callback(
            pid=1,
            tid=2,
            timestamp=11,
            callback_object=3,
            message=4,
            message_timestamp=6,
        )
        unify_callback_records(data)

        data_expect = Ros2DataModel()
        data_expect.add_intra_callback_duration(
            pid=1,
            tid=2,
            callback=3,
            callback_start_timestamp_raw=12,
            callback_end_timestamp_raw=13,
            callback_end_timestamp=13,
            message_timestamp=6,
        )

        assert data.intra_callback_duration.equals(data_expect.intra_callback_duration)


class TestPublishRecordsPostProcess:

    def test_unify_inter_publish_records(self):
        data = Ros2DataModel()
        data.add_rclcpp_publish(
            pid=1,
            tid=2,
            timestamp=4,
            publisher_handle=3,
            message=999,
            message_timestamp=8,
        )
        data.add_rcl_publish(
            pid=1,
            tid=2,
            timestamp=5,
            publisher_handle=3,
            message=999,
        )
        data.add_dds_write(
            pid=1,
            tid=2,
            timestamp=6,
            message=999
        )
        data.add_dds_bind_addr_to_stamp(
            pid=1,
            tid=2,
            timestamp=7,
            addr=99,
            source_timestamp=7
        )
        unify_publish_records(data)

        data_expect = Ros2DataModel()
        data_expect.set_offset(0, 0)
        data_expect.add_inter_publish(
            pid=1,
            tid=2,
            publisher_handle=3,
            rclcpp_publish_timestamp_raw=4,
            rcl_publish_timestamp_raw=5,
            dds_write_timestamp_raw=6,
            source_timestamp=7,
            message_timestamp=8,
        )

        assert len(data_expect.inter_publish) == 1
        assert data.inter_publish.equals(data_expect.inter_publish)

    def test_unify_inter_publish_records_without_optional(self):
        data = Ros2DataModel()
        data.add_rclcpp_publish(
            pid=1,
            tid=2,
            timestamp=4,
            publisher_handle=3,
            message=999,
            message_timestamp=8,
        )

        data.add_dds_bind_addr_to_stamp(
            pid=1,
            tid=2,
            timestamp=7,
            addr=99,
            source_timestamp=7
        )
        unify_publish_records(data)

        data_expect = Ros2DataModel()
        data_expect.set_offset(0, 0)
        data_expect.add_inter_publish(
            pid=1,
            tid=2,
            publisher_handle=3,
            rclcpp_publish_timestamp_raw=4,
            rcl_publish_timestamp_raw=None,
            dds_write_timestamp_raw=None,
            source_timestamp=7,
            message_timestamp=8,
        )

        assert len(data_expect.inter_publish) == 1
        assert data.inter_publish.equals(data_expect.inter_publish)


class TestTfLookupRecordsPostProcess:

    def test_unify_tf_lookup_records(self):
        data = Ros2DataModel()
        data.add_tf_lookup_transform_start(
            pid=1,
            tid=2,
            timestamp=4,
            buffer_core=3,
            target_time=7,
            target_frame_id_compact=6,
            source_frame_id_compact=7
        )
        data.add_tf_lookup_transform_end(
            pid=1,
            tid=2,
            timestamp=5,
            buffer_core=3,
        )

        unify_tf_lookup_records(data)

        data_expect = Ros2DataModel()

        data_expect.set_offset(0, 0)
        data_expect.add_tf_lookup_transform(
            pid=1,
            tid=2,
            buffer_core=3,
            lookup_transform_timestamp_start_raw=4,
            lookup_transform_timestamp_end=5,
            target_frame_id_compact=6,
            source_frame_id_compact=7,
        )

        assert len(data_expect.tf_lookup_transform) == 1
        assert data.tf_lookup_transform.equals(data_expect.tf_lookup_transform)
