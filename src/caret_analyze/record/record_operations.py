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

from enum import IntEnum
from itertools import groupby

from .interface import RecordsInterface
from ..exceptions import InvalidArgumentError


class MergeSide(IntEnum):
    LEFT = 0
    RIGHT = 1


def merge(
    left_records: RecordsInterface,
    right_records: RecordsInterface,
    join_left_key: str,
    join_right_key: str,
    columns: list[str],
    how: str,
) -> RecordsInterface:
    assert type(left_records) == type(right_records)

    return left_records.merge(
        right_records,
        join_left_key,
        join_right_key,
        columns,
        how,
    )


def merge_sequential(
    left_records: RecordsInterface,
    right_records: RecordsInterface,
    left_stamp_key: str,
    right_stamp_key: str,
    join_left_key: str | None,
    join_right_key: str | None,
    columns: list[str],
    how: str,
) -> RecordsInterface:
    assert type(left_records) == type(right_records)

    return left_records.merge_sequential(
        right_records,
        left_stamp_key,
        right_stamp_key,
        join_left_key,
        join_right_key,
        columns,
        how,
    )


class RecordType(IntEnum):
    SOURCE = (0,)
    COPY = (1,)
    SINK = 2


def merge_sequential_for_addr_track(
    source_records: RecordsInterface,
    source_stamp_key: str,
    source_key: str,
    copy_records: RecordsInterface,
    copy_stamp_key: str,
    copy_from_key: str,
    copy_to_key: str,
    sink_records: RecordsInterface,
    sink_stamp_key: str,
    sink_from_key: str,
    columns: list[str],
):
    assert type(source_records) == type(copy_records) and type(
        copy_records) == type(sink_records)

    return source_records.merge_sequential_for_addr_track(
        source_stamp_key,
        source_key,
        copy_records,
        copy_stamp_key,
        copy_from_key,
        copy_to_key,
        sink_records,
        sink_stamp_key,
        sink_from_key,
        columns,
    )


def validate_rename_rule(rename_rule: dict[str, str]):
    # overwrite columns
    if len(set(rename_rule.keys()) & set(rename_rule.values())) > 0:
        msg = 'Overwrite columns. '
        msg += str(rename_rule)
        raise InvalidArgumentError(msg)

    # duplicate columns after change
    for _, group in groupby(rename_rule.values()):
        if len(list(group)) > 1:
            msg = 'duplicate columns'
            msg += str(rename_rule)
            raise InvalidArgumentError(msg)
