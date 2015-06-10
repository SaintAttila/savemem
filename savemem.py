# savemem
# -------
# Disk-backed container types that permit limits on RAM use to be specified.
# Container types defined here are not meant to be persistent, but rather to be
# memory efficient by effectively taking advantage disk storage. They should act
# as drop-in replacements for containers in situations where RAM is at a premium
# and disk space is not.


__author__ = 'Aaron Hosford'
__version__ = '0.0.2'


import os
import pickle
import shelve
import shutil
import tempfile
import time
import threading

from collections.abc import MutableMapping, MutableSequence, MutableSet


def get_default_cache_limit():
    """Return the default maximum cache size per container."""
    return LowMemContainer._default_cache_limit

def set_default_cache_limit(limit):
    """Set the default maximum cache limit per container."""
    assert isinstance(limit, int)
    assert limit >= 0
    LowMemContainer._default_cache_limit = limit


class LowMemContainer:
    """
    Base class for non-persistent containers which make use of disk space to keep RAM usage below a maximum
    threshold.
    """

    _default_cache_limit = 100000

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

    def __init__(self, *args, **kwargs):
        self._dir = tempfile.mkdtemp()
        self._shelf = shelve.open(os.path.join(self._dir, 'lowmemdict'))
        self._open = True

        self._cache_limit = self._default_cache_limit
        self._cache = dict(*args, **kwargs)
        self._recency = {}

        self._flush_thread = None
        self._stop_flushing = False
        self._flush_exception = None

        self.flush()

    def __del__(self):
        self.close()

    @property
    def cache_limit(self):
        """The cache limit for this container. This is the maximum number of items (values or key/value pairs)
        that this container will hold in memory."""
        return self._cache_limit

    @property
    def cache_size(self):
        """The current actual cache size. This will never exceed the cache limit."""
        return len(self._cache)

    @property
    def is_open(self):
        """A Boolean flag indicating whether the container is open. Attempting to use it after it has been closed
        will cause an error to be raised."""
        return self._open

    def close(self):
        """Close the container. Attempting to use it after it has been closed will cause an error to be raised."""
        if not self._open:
            return

        # Close out the flush thread, if necessary.
        self._wait_for_flush(interrupt=True)

        if self._shelf is not None:
            self._shelf.close()
            self._shelf = None

        if self._dir is not None:
            shutil.rmtree(self._dir)
            self._dir = None

        if self._cache is not None:
            self._cache.clear()
            self._cache = None

        if self._recency is not None:
            self._recency.clear()
            self._recency = None

        self._open = False

    def _wait_for_flush(self, interrupt=False):
        if interrupt:
            self._stop_flushing = True

        # If there is already a flush in progress, wait for it to complete.
        if self._flush_thread:
            if self._flush_thread.is_alive() and not self._flush_thread is threading.current_thread():
                self._flush_thread.join()
            self._flush_thread = None

        self._stop_flushing = False

        if self._flush_exception:
            flush_exception = self._flush_exception
            self._flush_exception = None
            raise flush_exception

    def _do_flush(self, cache):
        """Moves contents of the given cache dict to disk."""
        try:
            while cache and not self._stop_flushing:
                key, value = cache.popitem()
                self._shelf[self._encode_key(key)] = value
            if cache:
                cache.clear()
        except BaseException as exception:
            self._flush_exception = exception

    def flush(self, synchronous=False, exclusions=None):
        """Completely remove all cached items from RAM and dump them to disk."""
        if not self._cache:
            return  # Nothing to do

        self._wait_for_flush()

        cache = self._cache

        if exclusions:
            recency = self._recency
            self._recency = {}
            for key in exclusions:
                self._cache[key] = cache.pop(key)
                self._recency[key] = recency.pop(key)
            del recency
        else:
            self._cache = {}
            self._recency.clear()

        self._flush_thread = threading.Thread(target=self._do_flush, args=(cache,))
        self._flush_thread.daemon = True
        self._flush_thread.start()

        if synchronous:
            del cache
            self._wait_for_flush()

    def _get(self, key):
        if key in self._cache:
            self._recency[key] = time.time()
            return self._cache[key]

        self._wait_for_flush()

        try:
            value = self._shelf[self._encode_key(key)]
        except KeyError:
            raise KeyError(key)

        if len(self._cache) >= self._cache_limit:
            self.flush()

        self._cache[key] = value
        self._recency[key] = time.time()

        return value

    def _set(self, key, value):
        if key in self._cache:
            self._cache[key] = value
            return

        if len(self._cache) >= self._cache_limit:
            self.flush()

        self._cache[key] = value
        self._recency[key] = time.time()

    def _del(self, key):
        if key in self._cache:
            del self._cache[key]
            del self._recency[key]

        self._wait_for_flush()

        try:
            del self._shelf[self._encode_key(key)]
        except KeyError:
            pass

    def __contains__(self, key):
        if key in self._cache:
            self._recency[key] = time.time()
            return True

        if key in self._recency:
            return True

        self._wait_for_flush()
        return self._encode_key(key) in self._shelf

    def __iter__(self):
        self._wait_for_flush()
        for key_str in self._shelf:
            yield self._decode_key(key_str)

    def __len__(self):
        return len(self._shelf)

    def clear(self):
        """Remove all items."""
        self._wait_for_flush(interrupt=True)
        self._cache.clear()
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
        try:
            self._del(item)
        except KeyError:
            pass


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
        try:
            counter = self._get(item) - 1
        except KeyError:
            return

        if counter <= 0:
            self._del(item)
        else:
            self._set(item, counter)

    def __iter__(self):
        self._wait_for_flush()
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

    __getitem__ = LowMemContainer._get
    __setitem__ = LowMemContainer._set
    __delitem__ = LowMemContainer._del

