class BaseCacheException(Exception):
    """base class for all cache exceptions."""

    ...


class GenericCacheReadError(BaseCacheException):
    """base class for any error during cache read."""

    ...


class GenericCacheWriteError(BaseCacheException):
    """base class for any error during cache write."""

    ...
