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

from collections.abc import Callable, Collection, Iterable

import difflib

import itertools

from logging import getLogger

import os

from statistics import mean
from typing import Any

from ..exceptions import ItemNotFoundError, MultipleItemFoundError

logger = getLogger(__name__)


class Util:

    @staticmethod
    def flatten(x: Iterable[Iterable[Any]]) -> list[Any]:
        """
        Expand double nested Iterable to List.

        Parameters
        ----------
        x : Iterable[Iterable[Any]]
            Target to flatten.

        Returns
        -------
        list[Any]
            Flattened list.

        """
        return list(itertools.chain.from_iterable(x))

    @staticmethod
    def filter_items(f: Callable[[Any], bool], x: Iterable[Any] | None) -> list[Any]:
        """
        Filter iterable.

        Parameters
        ----------
        f : Callable[[Any], bool]
            Filtering condition. Items that return True remain.
        x : Iterable[Any] | None
            Filtering target.

        Returns
        -------
        list[Any]
            Filtered list.

        """
        if x is None:
            return []
        return list(filter(f, x))

    @staticmethod
    def num_digit(i: int) -> int:
        """
        Get number of digits in decimal.

        Parameters
        ----------
        i : int
            number.

        Returns
        -------
        int
            digits.

        """
        return len(str(abs(i)))

    @staticmethod
    def ext(path: str) -> str:
        """
        Get extension from path.

        Parameters
        ----------
        path : str
            path name to get extension.

        Returns
        -------
        str
            extension.

        Note
        ----
            This function is duplicated. see: get_ext in Util.

        """
        _, ext = os.path.splitext(path)
        return ext[1:]

    @staticmethod
    def find_one(condition: Callable[[Any], bool], items: Iterable[Any] | None) -> Any:
        """
        Get a single item that matches the condition.

        Parameters
        ----------
        condition : Callable[[Any], bool]
            condition
        items : Iterable[Any] | None
            Items to be searched.

        Returns
        -------
        Any
            condition matched single item.

        Raises
        ------
        ItemNotFoundError
            Failed to find an item that matches the condition.
        MultipleItemFoundError
            Failed to identify an item that matches the condition.

        """
        if items is None:
            raise ItemNotFoundError('Failed find item.')

        filtered = Util.filter_items(condition, items)
        if len(filtered) == 0:
            raise ItemNotFoundError('Failed find item.')
        if len(filtered) >= 2:
            raise MultipleItemFoundError('Failed to identify item.')

        return filtered[0]

    @staticmethod
    def find_similar_one(target_name: str,
                         items: Collection[Any],
                         key: Callable[[Any], str] = lambda x: x,
                         th: float = 0.6
                         ) -> Any:
        """
        Get a single item that matches the condition.

        Parameters
        ----------
        target_name: str
            target_name
        items: Collection[Any]
            Items to be searched.
        key: Callable[[Any], str]
            key
        th: float
            Similarity judgment threshold.
            A candidate is mentioned only if it is higher than the threshold.

        Returns
        -------
        Any
            condition matched single item.

        Raises
        ------
        ItemNotFoundError
            Failed to find an item that matches the condition.

        """
        similarity = 0.0
        for item in items:
            distance = difflib.SequenceMatcher(None, key(item), target_name).ratio()
            if (distance > similarity):
                similarity = distance
                most_similar_item = item

        assert 0.0 <= similarity <= 1.0
        if (similarity == 1.0):
            return most_similar_item
        elif (similarity > th):
            msg = 'Arguments may be wrong.'
            msg += f" Isn't it '{key(most_similar_item)}'?"
            raise ItemNotFoundError(msg)
        else:
            raise ItemNotFoundError('Failed find item.')

    @staticmethod
    def find_similar_one_multi_keys(
        target_names: dict[str, str | int],
        items: Collection[Any],
        keys: Callable[[Any], dict[str, str | int]] = lambda x: x,
        th: float = 0.6
    ) -> Any:
        """
        Get a single item that matches the multi conditions.

        Parameters
        ----------
        target_names: dict[str, str | int]
            target_names
        items: Collection[Any]
            Items to be searched.
        keys: Callable[[Any], dict[str, str | int]]
            key
        th: float
            Similarity judgment threshold.
            A candidate is mentioned only if it is higher than the threshold.

        Returns
        -------
        Any
            conditions matched single item.

        Raises
        ------
        ItemNotFoundError
            Failed to find an item that matches the conditions.

        """
        max_similarity = 0.0
        for item in items:
            each_similarity = []
            keys_dict = keys(item)
            for target_name in target_names:
                if (keys_dict[target_name] is None):
                    each_similarity.append(0.0)
                    continue
                if type(keys_dict[target_name]) is not str:
                    if keys_dict[target_name] == target_names[target_name]:
                        distance = 1.0
                    else:
                        distance = 0.0
                    each_similarity.append(distance)
                    continue
                distance = difflib.SequenceMatcher(None,
                                                   str(keys_dict[target_name]),
                                                   str(target_names[target_name])).ratio()
                each_similarity.append(distance)
            if (mean(each_similarity) > max_similarity):
                max_similarity = mean(each_similarity)
                most_similar_item = item

        assert 0.0 <= max_similarity <= 1.0
        if (max_similarity == 1.0):
            return most_similar_item
        elif (max_similarity > th):
            msg = 'Arguments may be wrong. '
            msg += "Aren't they bellow?\n"
            keys_dict = keys(most_similar_item)
            for k, v in keys_dict.items():
                msg += k + "='" + str(v) + "'\n"
            raise ItemNotFoundError(msg)
        else:
            raise ItemNotFoundError('Failed find item.')

    @staticmethod
    def ns_to_ms(x: float) -> float:
        """
        Convert nanosecond to millisecond.

        Parameters
        ----------
        x : float
            time in nano-second.

        Returns
        -------
        float
            time in millisecond.

        """
        return x * 1.0e-6

    @staticmethod
    def get_ext(path: str) -> str:
        """
        Get extension from path.

        Parameters
        ----------
        path : str
            path name to get extension.

        Returns
        -------
        str
            extension.

        Note
        ----
            This function is duplicated. see: ext in Util.

        """
        return os.path.basename(path).split('.')[-1]

    @staticmethod
    def to_ns_and_name(nodename: str) -> tuple[str, str]:
        """
        Convert fully qualified node name.

        Parameters
        ----------
        nodename : str
            fully qualified node name.

        Returns
        -------
        tuple[str, str]
            name space, node name.

        """
        strs = nodename.split('/')
        ns = '/'.join(strs[:-1]) + '/'
        name = strs[-1]
        return ns, name
