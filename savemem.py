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

from collections.abc import MutableMapping


class LowMemDict(MutableMapping):
    """
    Non-persistent dictionary which makes use of disk space to keep RAM usage below a maximum threshold. The default
    threshold is 10000 bytes.
    """

    def __init__(self, *args, **kwargs):
        self._dir = tempfile.mkdtemp()
        self._shelf = shelve.open(os.path.join(self._dir, 'lowmemdict'))
        self._open = True
        self._max_memory = 10000
        self._contents = dict(*args, **kwargs)
        self._recency = dict.fromkeys(self._contents, time.time())

    def get_size(self):
        """Return the total size in bytes of this container. Note that if keys or values are composite objects, only
        the size of the root-level object, not its members, will be accounted for."""
        return (
            sys.getsizeof(self) +
            sys.getsizeof(self._shelf) +
            sys.getsizeof(self._open) +
            sys.getsizeof(self._max_memory) +
            sys.getsizeof(self._contents) +
            sys.getsizeof(self._recency) +
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
        """A Boolean flag indicating whether the dictionary is open."""
        return self._open

    def __del__(self):
        self.close()

    def close(self):
        """Close the container. Attempting to use it after it has been closed will cause a ValueError to be raised."""
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

    def clear_cache(self):
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
        if max_memory == 0:
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
        current_size = self.get_size()
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
            current_size = self.get_size()

    def __getitem__(self, key):
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

    def __setitem__(self, key, value):
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

    def __delitem__(self, key):
        if key in self._contents:
            del self._contents[key]
            del self._recency[key]
        else:
            key_str = self._encode_key(key)
            try:
                del self._shelf[key_str]
            except KeyError:
                raise KeyError(key)

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
