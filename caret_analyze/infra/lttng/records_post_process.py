
from .column_names import COLUMN_NAME
from .ros2_tracing.data_model import Ros2DataModel
from ...record import merge_sequencial


def post_process_records(data: Ros2DataModel):
    unify_callback_records(data)
    unify_publish_records(data)
    unify_tf_lookup_records(data)


def unify_callback_records(data: Ros2DataModel):
    join_keys = [
        COLUMN_NAME.PID,
        COLUMN_NAME.CALLBACK_OBJECT,
        COLUMN_NAME.TID
    ]
    callback_duration = merge_sequencial(
        left_records=data.callback_start,
        right_records=data.callback_end,
        left_stamp_key=COLUMN_NAME.CALLBACK_START_TIMESTAMP,
        right_stamp_key=COLUMN_NAME.CALLBACK_END_TIMESTAMP,
        join_left_key=join_keys,
        join_right_key=join_keys,
        how='inner'
    )

    callback_records = callback_duration.groupby([COLUMN_NAME.IS_INTRA_PROCESS])

    if (0,) in callback_records:
        inter_records = callback_records[(0,)]
        join_keys = [
            COLUMN_NAME.PID,
            COLUMN_NAME.TID,
            COLUMN_NAME.CALLBACK_OBJECT,
        ]
        inter_callback = merge_sequencial(
            left_records=data.dispatch_subscription_callback,
            right_records=inter_records,
            left_stamp_key=COLUMN_NAME.DISPATCH_SUBSCRIPTION_CALLBACK_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            join_left_key=join_keys,
            join_right_key=join_keys,
            how='inner'
        )
        drop_columns = set(inter_callback.column_names) - set(
            data.inter_callback_duration.column_names)
        inter_callback.columns.drop(drop_columns)
        data.inter_callback_duration.concat(inter_callback)
    if (1,) in callback_records:
        join_keys = [
            COLUMN_NAME.PID,
            COLUMN_NAME.TID,
            COLUMN_NAME.CALLBACK_OBJECT,
        ]
        intra_records = callback_records[(1,)]
        intra_callback = merge_sequencial(
            left_records=data.dispatch_intra_process_subscription_callback,
            right_records=intra_records,
            left_stamp_key=COLUMN_NAME.DISPATCH_INTRA_PROCESS_SUBSCRIPTION_CALLBACK_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            join_left_key=join_keys,
            join_right_key=join_keys,
            how='inner'
        )
        drop_columns = set(intra_callback.column_names) - set(
            data.intra_callback_duration.column_names)
        intra_callback.columns.drop(drop_columns)
        data.intra_callback_duration.concat(intra_callback)


def unify_publish_records(data: Ros2DataModel):
    join_keys = [COLUMN_NAME.PID, COLUMN_NAME.TID]
    inter_publish = merge_sequencial(
        left_records=data.rclcpp_publish,
        right_records=data.rcl_publish,
        left_stamp_key=COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
        right_stamp_key=COLUMN_NAME.RCL_PUBLISH_TIMESTAMP,
        join_left_key=join_keys,
        join_right_key=join_keys,
        how='left'
    )

    inter_publish = merge_sequencial(
        left_records=inter_publish,
        right_records=data.dds_write,
        left_stamp_key=COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
        right_stamp_key=COLUMN_NAME.DDS_WRITE_TIMESTAMP,
        join_left_key=join_keys,
        join_right_key=join_keys,
        how='left'
    )
    inter_publish = merge_sequencial(
        left_records=inter_publish,
        right_records=data.dds_bind_addr_to_stamp,
        left_stamp_key=COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
        right_stamp_key=COLUMN_NAME.DDS_BIND_ADDR_TO_STAMP_TIMESTAMP,
        join_left_key=join_keys,
        join_right_key=join_keys,
        how='inner'
    )

    drop_columns = set(inter_publish.column_names) - set(
        data.inter_publish.column_names)
    inter_publish.columns.drop(drop_columns)
    data.inter_publish.concat(inter_publish)


def unify_tf_lookup_records(data: Ros2DataModel):
    join_keys = [COLUMN_NAME.PID, COLUMN_NAME.TID]

    tf_lookup_transform = merge_sequencial(
        left_records=data.tf_lookup_transform_start,
        right_records=data.tf_lookup_transform_end,
        left_stamp_key=COLUMN_NAME.TF_LOOKUP_TRANSFORM_START_TIMESTAMP,
        right_stamp_key=COLUMN_NAME.TF_LOOKUP_TRANSFORM_END_TIMESTAMP,
        join_left_key=join_keys,
        join_right_key=join_keys,
        how='inner'
    )

    drop_columns = set(tf_lookup_transform.column_names) - set(
        data.tf_lookup_transform.column_names)
    tf_lookup_transform.columns.drop(drop_columns)
    data.tf_lookup_transform.concat(tf_lookup_transform)
