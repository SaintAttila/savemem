# savemem
# -------
# Disk-backed container types that permit limits on RAM use to be specified.
# Container types defined here are not meant to be persistent, but rather to be
# memory efficient by effectively taking advantage disk storage. They should act
# as drop-in replacements for containers in situations where RAM is at a premium
# and disk space is not.


__author__ = 'Aaron Hosford'
__version__ = '0.0.1'


import math
import os
import pickle
import shelve
import shutil
import sys
import tempfile
import time

from collections.abc import MutableMapping, MutableSequence, MutableSet


def get_default_max_memory():
    """Return the default maximum memory per container."""
    return LowMemContainer._default_max_memory

def set_default_max_memory(max_memory):
    """Set the default maximium memory per container."""
    assert isinstance(max_memory, int)
    assert max_memory >= 0
    LowMemContainer._default_max_memory = max_memory


class LowMemContainer:
    """
    Base class for non-persistent containers which make use of disk space to keep RAM usage below a maximum
    threshold.
    """

    _default_max_memory = 100000000

    def __init__(self, *args, **kwargs):
        self._dir = tempfile.mkdtemp()
        self._shelf = shelve.open(os.path.join(self._dir, 'lowmemdict'))
        self._open = True
        self._max_memory = self._default_max_memory
        self._contents = dict(*args, **kwargs)
        self._recency = dict.fromkeys(self._contents, time.time())
        self._auto_flush = False
        self._trim_memory()

    def get_memory_usage(self):
        """Return the total size in bytes of this container. Note that if keys or values are composite objects, only
        the size of the root-level object, not its members, will be accounted for."""
        return (
            sys.getsizeof(self) +
            sys.getsizeof(self._shelf) +
            sys.getsizeof(self._open) +
            sys.getsizeof(self._max_memory) +
            sys.getsizeof(self._contents) +
            sys.getsizeof(self._recency) +
            sys.getsizeof(self._auto_flush) +
            sum(sys.getsizeof(key) for key in self._contents) +
            sum(sys.getsizeof(value) for value in self._contents.values()) +
            sum(sys.getsizeof(value) for value in self._recency.values())
        )

    @staticmethod
    def _encode_key(key):
        key_bytes = pickle.dumps(key, pickle.HIGHEST_PROTOCOL)
        del key
        key_str = ''.join(chr(byte) for byte in key_bytes)
        del key_bytes
        return key_str

    @staticmethod
    def _decode_key(key_str):
        key_bytes = bytes(ord(char) for char in key_str)
        del key_str
        key = pickle.loads(key_bytes)
        del key_bytes
        return key

    @property
    def is_open(self):
        """A Boolean flag indicating whether the container is open. Attempting to use it after it has been closed
        will cause an error to be raised."""
        return self._open

    def _get_auto_flush(self):
        return self._auto_flush

    def _set_auto_flush(self, value):
        assert value in (0, 1)
        self._auto_flush = bool(value)

    auto_flush = property(
        _get_auto_flush,
        _set_auto_flush,
        doc="A Boolean flag indicating whether flush() should automatically be called when memory "
            "usage reaches its max."
    )

    def __del__(self):
        self.close()

    def close(self):
        """Close the container. Attempting to use it after it has been closed will cause an error to be raised."""
        if not self._open:
            return

        if self._contents is not None:
            self._contents.clear()
            self._contents = None

        if self._recency is not None:
            self._recency.clear()
            self._recency = None

        if self._shelf is not None:
            self._shelf.close()
            self._shelf = None

        if self._dir is not None:
            shutil.rmtree(self._dir)
            self._dir = None

        self._max_memory = None
        self._open = False

    def flush(self):
        """Completely remove all cached items from RAM and dump them to disk."""
        self._trim_memory(0)

    def _get_max_memory(self):
        return self._max_memory

    def _set_max_memory(self, max_memory):
        self._max_memory = max_memory
        self._trim_memory()

    max_memory = property(
        _get_max_memory,
        _set_max_memory,
        doc="The maximum amount of RAM the container is permitted to use."
    )

    def _trim_memory(self, max_memory=None):
        """Moves contents out of RAM and on to disk until the maximum memory threshold is met."""
        if max_memory is None:
            max_memory = self._max_memory

        assert max_memory >= 0

        # If we are completely clearing the cache, there is a faster way.
        if max_memory == 0 or (self._auto_flush and self.get_memory_usage() > self._max_memory):
            self._recency.clear()
            while self._contents:
                key, value = self._contents.popitem()
                key_str = self._encode_key(key)
                del key
                self._shelf[key_str] = value
                del key_str
                del value
            return

        # Otherwise, estimate how much we need to remove, remove it, and then check again until
        # it is below the threshold.
        current_size = self.get_memory_usage()
        while self._contents and current_size > max_memory:
            fraction_to_remove = (current_size - max_memory) / current_size
            number_to_remove = math.ceil(fraction_to_remove * len(self._contents))
            for _ in range(number_to_remove):
                # Move the least recently used item out of memory.
                lru_key = min(self._recency, key=self._recency.get)
                lru_value = self._contents.pop(lru_key)
                del self._recency[lru_key]

                # And on to disk
                key_str = self._encode_key(lru_key)
                self._shelf[key_str] = lru_value

                del key_str
                del lru_value
                del lru_key
            current_size = self.get_memory_usage()

    def _get(self, key):
        if key in self._contents:
            value = self._contents[key]
            self._recency[key] = time.time()
            del key
        else:
            key_str = self._encode_key(key)
            try:
                value = self._shelf.pop(key_str)
            except KeyError:
                raise KeyError(key)
            del key_str
            self._contents[key] = value
            self._recency[key] = time.time()
            self._trim_memory()
            del key
        return value

    def _set(self, key, value):
        if key in self._contents:
            old_value = self._contents[key]
            self._contents[key] = value
            self._recency[key] = time.time()
            if sys.getsizeof(old_value) < sys.getsizeof(value):
                self._trim_memory()
            del key, old_value, value
        else:
            key_str = self._encode_key(key)
            if key_str in self._shelf:
                del self._shelf[key_str]
            del key_str
            self._contents[key] = value
            self._recency[key] = time.time()
            self._trim_memory()
            del key, value

    def _del(self, key):
        if key in self._contents:
            del self._contents[key]
            del self._recency[key]
        else:
            key_str = self._encode_key(key)
            try:
                del self._shelf[key_str]
            except KeyError:
                raise KeyError(key)

    def __contains__(self, item):
        if item in self._contents:
            return True
        key_str = self._encode_key(item)
        return key_str in self._shelf

    def __iter__(self):
        for key in self._contents:
            yield key
        for key_str in self._shelf:
            yield self._decode_key(key_str)

    def __len__(self):
        return len(self._contents) + len(self._shelf)

    def clear(self):
        """Remove all items."""
        self._contents.clear()
        self._recency.clear()
        self._shelf.clear()


class LowMemSet(LowMemContainer, MutableSet):
    """
    Non-persistent set which makes use of disk space to keep RAM usage below a maximum threshold. The default
    threshold is 10000 bytes.
    """

    def add(self, item):
        """Add an item to the set. If it is already present, do nothing."""
        self._set(item, None)

    def discard(self, item):
        """Remove an item from the set. If it is not present, do nothing."""
        if item in self:
            self._del(item)


class LowMemMultiSet(LowMemContainer, MutableSet):
    """
    Non-persistent multiset which makes use of disk space to keep RAM usage below a maximum threshold. The default
    threshold is 10000 bytes.
    """

    def add(self, item):
        """Add an item to the multiset."""
        if item in self:
            self._set(item, self._get(item) + 1)
        else:
            self._set(item, 1)

    def discard(self, item):
        """Remove an item from the set. If it is not present, do nothing."""
        if item in self:
            counter = self._get(item) - 1
            if counter <= 0:
                self._del(item)
            else:
                self._set(item, counter)

    def __iter__(self):
        for key, count in self._contents.items():
            for _ in range(count):
                yield key
        for key_str, count in self._shelf.items():
            key = self._decode_key(key_str)
            for _ in range(count):
                yield key


# TODO: This one in particular is rather ugly. It needs some work.
class LowMemList(LowMemContainer, MutableSequence):
    """
    Non-persistent list which makes use of disk space to keep RAM usage below a maximum threshold. The default
    threshold is 10000 bytes.
    """

    def _convert_index(self, index):
        assert isinstance(index, int)
        length = len(self)
        if index < 0:
            index += length
        if 0 <= index < length:
            return index
        else:
            raise IndexError(index)

    def __getitem__(self, item):
        if isinstance(item, slice):
            raise NotImplementedError("Slices are not supported.")
        return self._get(self._convert_index(item))

    def __setitem__(self, key, value):
        if isinstance(key, slice):
            raise NotImplementedError("Slices are not supported.")
        self._set(self._convert_index(key), value)

    def __delitem__(self, key):
        # TODO: Figure out a good way to work around this sort of thing without resorting to rewriting the entire DB.
        if isinstance(key, slice):
            raise NotImplementedError("Slices are not supported.")
        index = self._convert_index(key)
        if index == len(self) - 1:
            self._del(index)
        else:
            raise NotImplementedError("Deletion is not supported except at the end of a list.")

    def insert(self, index, value):
        index = self._convert_index(index)
        if index == len(self):
            self._set(index, value)
        else:
            raise NotImplementedError("Insertion is not supported except at the end of a list.")

    def trim(self, length):
        """Trim any items with index >= length."""
        assert isinstance(length, int)
        assert length >= 0

        # Remove in reverse order to preserve self-consistency in case of an error.
        for index in range(len(self) - 1, length - 1, -1):
            self._del(index)


class LowMemDict(LowMemContainer, MutableMapping):
    """
    Non-persistent dictionary which makes use of disk space to keep RAM usage below a maximum threshold. The default
    threshold is 10000 bytes.
    """

    def __getitem__(self, key):
        return self._get(key)

    def __setitem__(self, key, value):
        self._set(key, value)

    def __delitem__(self, key):
        self._del(key)

