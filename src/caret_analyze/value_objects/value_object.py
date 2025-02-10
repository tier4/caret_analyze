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

import inspect

from inspect import isclass, getmro

import types

from typing import Any

from yaml import dump


class ValueObject():
    """
    Value object base class.

    Classes that inherit from this class will become immutable ValueObjects,
    and __eq__ and __hash__ will be calculated based on public properties.

    Note:
    ----
    Since the hash value is immutable, inherited classes can be used as a dictionary type key.
    It is also suitable for cache use and does not unintentionally change properties.

    """

    def __eq__(self, right: Any) -> bool:
        """
        Check whether self object equals to given instance [override].

        Parameters
        ----------
        right : Any
            Comparison target.

        Returns
        -------
        bool
            Recursively compares the values of the published properties and
            returns True only if they all match. False otherwise.

        """
        if type(self) != type(right):
            return False

        #for attr in self.__generate_public_attrs():
        for attr in self.__generate_public_attrs_revise():
            # Uncomment this when investigating why equals is false during test execution.
            # assert getattr(self,  attr) == getattr(right, attr)
            if getattr(self,  attr) != getattr(right, attr):
                return False
        return True

    def __hash__(self) -> int:
        """
        Calculate hash value.

        Returns
        -------
        int
            A hash value calculated from all of the publicly available
            property values by recursively referencing them.

        References
        ----------
            https://www.baeldung.com/java-hashcode

        """
        hash_value = 17

        hash_value += hash_value * 31 + hash(self.__class__)

        #for attr in self.__generate_public_attrs():
        for attr in self.__generate_public_attrs_revise():
            v = getattr(self,  attr)
            hash_value += hash_value * 31 + hash(v)

        return hash_value

    def __str__(self) -> str:
        """
        Convert to string.

        Returns
        -------
        str
            Yaml format strings created by recursively access properties.

        """
        d = self._to_dict()
        return dump(d)

    def _to_dict(self) -> dict[Any, Any]:
        """
        Convert to dictionary.

        Returns
        -------
        dict
            Dictionary created by recursively access properties.

        """
        d: dict[Any, Any] = {}
        for attr in self.__generate_public_attrs_revise():
            value = getattr(self, attr)
            if isinstance(value, ValueObject):
                d[attr] = value._to_dict()
            else:
                if isinstance(value, tuple):
                    d[attr] = list(value)
                else:
                    d[attr] = value
        return d


    def _getmembers_revise(self, object, predicate=None):
        """Return all members of an object as (name, value) pairs sorted by name.
        Optionally, only return members that satisfy a given predicate."""

        results = []
        names = set(dir(object))  # for no duplicates

        # inspect.py の getmembers() をリバイズ
        if isclass(object):
            mro = (object,) + getmro(object)
            # 基底クラスの __dict__ を事前に処理
            base_dicts = [base.__dict__ for base in mro if hasattr(base, '__dict__')]

            # DynamicClassAttribute を names に追加
            for base_dict in base_dicts:
                for k, v in base_dict.items():
                    if isinstance(v, types.DynamicClassAttribute):
                        names.add(k)
        else:
            mro = ()
            base_dicts = []  # クラスでない場合は空リスト

        # objectの __dict__
        if hasattr(object, '__dict__'):
            object_dict = object.__dict__
            for key in set(names):
                if key in object_dict:
                    value = object_dict[key]
                    if not predicate or predicate(value):
                        results.append((key, value))
                        names.remove(key)

        # base_dicts を処理 (object の__dict_ 以外？)
        for base_dict in base_dicts:
            for key in set(names):
                if key in base_dict:
                    value = base_dict[key]
                    if not predicate or predicate(value):
                        results.append((key, value))
                        names.remove(key)

        # スロットメンバのチェック
        for key in names:
            try:
                value = getattr(object, key)
                if not predicate or predicate(value):
                    results.append((key, value))
            except AttributeError:
                # could be a (currently) missing slot member, or a buggy
                # __dir__; discard and move on
                pass

        results.sort(key=lambda pair: pair[0])
        return results

    def __generate_public_attrs_revise(self):
        # attrs = inspect.getmembers(self)
        attrs = self._getmembers_revise(self)

        for key, value in attrs:
            if key[0] != '_' and key[0].islower() and not callable(value):
                yield key
    
    """
    def __generate_public_attrs(self):
        #attrs = inspect.getmembers(self)
        attrs = self._getmembers_revise(self)

        # ignore private variables and Constant variables
        attrs = list(filter(
            lambda x: x[0][0] != '_' and x[0][0].islower(), attrs
        ))
        for attr in attrs:
            key, value = attr[0], attr[1]
            # ignore callable
            if callable(value):
                continue
            yield key
    """
