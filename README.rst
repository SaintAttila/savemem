savemem
-------
Disk-backed non-persistent containers for memory efficiency

The savemem module provides disk-backed container types that permit limits on RAM use to be specified.
Container types defined in this module are not meant to be persistent, but rather to be memory efficient
by effectively taking advantage disk storage. When possible, they are intended to act as drop-in
replacements for Python's built-in containers in situations where RAM is at a premium and disk space is not.
