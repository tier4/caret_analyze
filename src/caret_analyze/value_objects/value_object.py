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
        for attr in self.__generate_public_attrs_fast():
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
        for attr in self.__generate_public_attrs_fast():
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
        for attr in self.__generate_public_attrs_fast():
            value = getattr(self, attr)
            if isinstance(value, ValueObject):
                d[attr] = value._to_dict()
            else:
                if isinstance(value, tuple):
                    d[attr] = list(value)
                else:
                    d[attr] = value
        return d

    def _getmembers_fast(self, object):
        """Return all members of an object as (name, value) pairs sorted by name.   
        Fast simplification of getmembers() in inspect.py."""

        results = []
        names = dir(object)

        if isclass(object):
            mro = (object,) + getmro(object)
            # Preprocessing __dict__ of base class
            base_dicts = [base.__dict__ for base in mro if hasattr(base, '__dict__')]
            # Add DynamicClassAttribute to names
            for base_dict in base_dicts:
                for k, v in base_dict.items():
                    if isinstance(v, types.DynamicClassAttribute):
                        names.append(k)
        else:
            # not class
            base_dicts = []

        names_set = set(names) # no duplicates

        # Process __dict__ of object
        if hasattr(object, '__dict__'):
            object_dict = object.__dict__
            for key in names_set & object_dict.keys():
                value = object_dict[key]
                results.append((key, value))

            names_set -= object_dict.keys()

        # Process all but class
        for base_dict in base_dicts:
            for key in names_set & base_dict.keys():
                value = base_dict[key]
                results.append((key, value))

            names_set -= base_dict.keys()

        # Check slot members
        for key in names_set:
            try:
                value = getattr(object, key)
                results.append((key, value))
            except AttributeError:
                # could be a (currently) missing slot member, or a buggy
                # __dir__; discard and move on
                pass

        results.sort(key=lambda pair: pair[0])
        return results

    # cache
    _public_attrs_cache = None
    def __generate_public_attrs_fast(self):
        if self._public_attrs_cache is None:
            attrs = self._getmembers_fast(self)
            self._public_attrs_cache = tuple(
                key for key, value in attrs
                if key[0] != '_' and key[0].islower() and not callable(value)
            )
        yield from self._public_attrs_cache 
    