import fnmatch
import time
from contextlib import asynccontextmanager, contextmanager
from typing import AsyncIterator, Iterator, Literal, Optional, override

from pydantic import ConfigDict, Field

from ab_core.cache.codec import DecodedT, EncodedT, safe_decode, safe_encode
from ab_core.cache.exceptions import GenericCacheReadError, GenericCacheWriteError

from ..schema.cache_type import CacheType
from .base import CacheAsyncSession, CacheBase, CacheSession


class InMemoryCacheSession(CacheSession):
    """
    Synchronous in-memory session.

    - Keys are namespaced strings (via CacheNamespace).
    - Values are stored as EncodedT (bytes-like).
    - Expiration tracked as epoch seconds in `expiry` (None means no expiration).
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    store: dict[str, EncodedT] = Field(default_factory=dict)
    expiry: dict[str, Optional[float]] = Field(default_factory=dict)

    def _cleanup_key(self, k: str) -> None:
        exp = self.expiry.get(k)
        if exp is not None and time.time() >= exp:
            self.store.pop(k, None)
            self.expiry.pop(k, None)

    @override
    def get(self, key: str) -> DecodedT:
        k = self.namespace.apply(key)
        try:
            self._cleanup_key(k)
            if k not in self.store:
                raise KeyError(f"No data found for key `{k}`")
            return safe_decode(self.store[k])
        except KeyError:
            raise
        except Exception as e:
            raise GenericCacheReadError(e) from e

    @override
    def set(self, key: str, value, expiry: Optional[int] = None) -> bool:
        k = self.namespace.apply(key)
        try:
            self.store[k] = safe_encode(value)
            self.expiry[k] = time.time() + expiry if expiry is not None else None
            return True
        except Exception as e:
            raise GenericCacheWriteError(e) from e

    @override
    def set_if_not_exists(self, key: str, value, expiry: Optional[int] = None) -> bool:
        k = self.namespace.apply(key)
        try:
            self._cleanup_key(k)
            if k in self.store:
                return False
            self.store[k] = safe_encode(value)
            self.expiry[k] = time.time() + expiry if expiry is not None else None
            return True
        except Exception as e:
            raise GenericCacheWriteError(e) from e

    @override
    def delete(self, key: str) -> int:
        k = self.namespace.apply(key)
        try:
            self._cleanup_key(k)
            existed = k in self.store
            self.store.pop(k, None)
            self.expiry.pop(k, None)
            return 1 if existed else 0
        except Exception as e:
            raise GenericCacheWriteError(e) from e

    @override
    def increment(
        self,
        key: str,
        *,
        increment_by: int = 1,
        initial_value: Optional[int] = None,
        expiry: Optional[int] = None,
    ) -> int:
        k = self.namespace.apply(key)
        try:
            self._cleanup_key(k)
            if k not in self.store:
                new_val = int(initial_value or 0)
            else:
                try:
                    current = int(safe_decode(self.store[k]))
                except Exception as conv:
                    raise GenericCacheWriteError(f"Value for key `{k}` is not an integer") from conv
                new_val = current + int(increment_by)

            # store as bytes (stringified integer)
            self.store[k] = str(new_val).encode("utf-8")
            if expiry is not None:
                self.expiry[k] = time.time() + expiry
            return new_val
        except GenericCacheWriteError:
            raise
        except Exception as e:
            raise GenericCacheWriteError(e) from e

    @override
    def get_keys(self, pattern: str = "*") -> list[str]:
        pat = self.namespace.apply(pattern)
        try:
            out: list[str] = []
            # Work on a snapshot since we might mutate in _cleanup_key
            for k in list(self.store.keys()):
                self._cleanup_key(k)
                if fnmatch.fnmatch(k, pat):
                    out.append(self.namespace.strip(k))
            return out
        except Exception as e:
            raise GenericCacheReadError(e) from e

    @override
    def delete_keys(self, pattern: str = "*") -> int:
        pat = self.namespace.apply(pattern)
        try:
            deleted = 0
            for k in list(self.store.keys()):
                if fnmatch.fnmatch(k, pat):
                    self.store.pop(k, None)
                    self.expiry.pop(k, None)
                    deleted += 1
            return deleted
        except Exception as e:
            raise GenericCacheWriteError(e) from e

    @override
    def get_ttl(self, key: str) -> int:
        k = self.namespace.apply(key)
        try:
            self._cleanup_key(k)
            if k not in self.store:
                raise KeyError(f"No data found for key `{k}`")
            exp = self.expiry.get(k)
            if exp is None:
                return -1
            ttl = int(exp - time.time())
            if ttl < 0:
                # expired between checks; treat as missing now
                self.store.pop(k, None)
                self.expiry.pop(k, None)
                raise KeyError(f"No data found for key `{k}`")
            return ttl
        except KeyError:
            raise
        except Exception as e:
            raise GenericCacheReadError(e) from e

    @override
    def expire(self, key: str, ttl: int) -> bool:
        k = self.namespace.apply(key)
        try:
            self._cleanup_key(k)
            if k not in self.store:
                return False
            self.expiry[k] = time.time() + int(ttl)
            return True
        except Exception as e:
            raise GenericCacheWriteError(e) from e

    @override
    def close(self) -> None:
        # Nothing to release for in-memory; keep API parity
        return None


class InMemoryCacheAsyncSession(CacheAsyncSession):
    """
    Asynchronous in-memory session.

    Independent store/expiry per session instance (matches the sync behaviour).
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    store: dict[str, EncodedT] = Field(default_factory=dict)
    expiry: dict[str, Optional[float]] = Field(default_factory=dict)

    def _cleanup_key(self, k: str) -> None:
        exp = self.expiry.get(k)
        if exp is not None and time.time() >= exp:
            self.store.pop(k, None)
            self.expiry.pop(k, None)

    @override
    async def get(self, key: str) -> DecodedT:
        k = self.namespace.apply(key)
        try:
            self._cleanup_key(k)
            if k not in self.store:
                raise KeyError(f"No data found for key `{k}`")
            return safe_decode(self.store[k])
        except KeyError:
            raise
        except Exception as e:
            raise GenericCacheReadError(e) from e

    @override
    async def set(self, key: str, value, expiry: Optional[int] = None) -> bool:
        k = self.namespace.apply(key)
        try:
            self.store[k] = safe_encode(value)
            self.expiry[k] = time.time() + expiry if expiry is not None else None
            return True
        except Exception as e:
            raise GenericCacheWriteError(e) from e

    @override
    async def set_if_not_exists(self, key: str, value, expiry: Optional[int] = None) -> bool:
        k = self.namespace.apply(key)
        try:
            self._cleanup_key(k)
            if k in self.store:
                return False
            self.store[k] = safe_encode(value)
            self.expiry[k] = time.time() + expiry if expiry is not None else None
            return True
        except Exception as e:
            raise GenericCacheWriteError(e) from e

    @override
    async def delete(self, key: str) -> int:
        k = self.namespace.apply(key)
        try:
            self._cleanup_key(k)
            existed = k in self.store
            self.store.pop(k, None)
            self.expiry.pop(k, None)
            return 1 if existed else 0
        except Exception as e:
            raise GenericCacheWriteError(e) from e

    @override
    async def increment(
        self,
        key: str,
        *,
        increment_by: int = 1,
        initial_value: Optional[int] = None,
        expiry: Optional[int] = None,
    ) -> int:
        k = self.namespace.apply(key)
        try:
            self._cleanup_key(k)
            if k not in self.store:
                new_val = int(initial_value or 0)
            else:
                try:
                    current = int(safe_decode(self.store[k]))
                except Exception as conv:
                    raise GenericCacheWriteError(f"Value for key `{k}` is not an integer") from conv
                new_val = current + int(increment_by)

            self.store[k] = str(new_val).encode("utf-8")
            if expiry is not None:
                self.expiry[k] = time.time() + expiry
            return new_val
        except GenericCacheWriteError:
            raise
        except Exception as e:
            raise GenericCacheWriteError(e) from e

    @override
    async def get_keys(self, pattern: str = "*") -> list[str]:
        pat = self.namespace.apply(pattern)
        try:
            out: list[str] = []
            for k in list(self.store.keys()):
                self._cleanup_key(k)
                if fnmatch.fnmatch(k, pat):
                    out.append(self.namespace.strip(k))
            return out
        except Exception as e:
            raise GenericCacheReadError(e) from e

    @override
    async def delete_keys(self, pattern: str = "*") -> int:
        pat = self.namespace.apply(pattern)
        try:
            deleted = 0
            for k in list(self.store.keys()):
                if fnmatch.fnmatch(k, pat):
                    self.store.pop(k, None)
                    self.expiry.pop(k, None)
                    deleted += 1
            return deleted
        except Exception as e:
            raise GenericCacheWriteError(e) from e

    @override
    async def get_ttl(self, key: str) -> int:
        k = self.namespace.apply(key)
        try:
            self._cleanup_key(k)
            if k not in self.store:
                raise KeyError(f"No data found for key `{k}`")
            exp = self.expiry.get(k)
            if exp is None:
                return -1
            ttl = int(exp - time.time())
            if ttl < 0:
                self.store.pop(k, None)
                self.expiry.pop(k, None)
                raise KeyError(f"No data found for key `{k}`")
            return ttl
        except KeyError:
            raise
        except Exception as e:
            raise GenericCacheReadError(e) from e

    @override
    async def expire(self, key: str, ttl: int) -> bool:
        k = self.namespace.apply(key)
        try:
            self._cleanup_key(k)
            if k not in self.store:
                return False
            self.expiry[k] = time.time() + int(ttl)
            return True
        except Exception as e:
            raise GenericCacheWriteError(e) from e

    @override
    async def close(self) -> None:
        # Nothing to release for in-memory; keep API parity
        return None


class InMemoryCache(CacheBase[InMemoryCacheSession, InMemoryCacheAsyncSession]):
    type: Literal[CacheType.INMEMORY] = CacheType.INMEMORY

    @override
    @contextmanager
    def sync_session(
        self,
        *,
        current_session: Optional[InMemoryCacheSession] = None,
    ) -> Iterator[InMemoryCacheSession]:
        if current_session:
            yield current_session
        else:
            with InMemoryCacheSession(namespace=self.namespace) as session:
                yield session

    @override
    @asynccontextmanager
    async def async_session(
        self,
        *,
        current_session: Optional[InMemoryCacheAsyncSession] = None,
    ) -> AsyncIterator[InMemoryCacheAsyncSession]:
        if current_session:
            yield current_session
        else:
            async with InMemoryCacheAsyncSession(namespace=self.namespace) as session:
                yield session
