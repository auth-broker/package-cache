import asyncio
import fnmatch
import time
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Literal, override

from pydantic import ConfigDict, Field, model_validator

from ab_core.cache.codec import DecodedT, safe_decode, safe_encode
from ab_core.cache.exceptions import GenericCacheReadError, GenericCacheWriteError

from ..schema.cache_type import CacheType
from .base import CacheAsyncSession, CacheBase, CacheSession

# ---------- Optional imports & typing ----------
if TYPE_CHECKING:
    import diskcache as diskcache  # for type checkers
else:
    diskcache = None  # type: ignore[assignment]

_HAS_DISKCACHE = False

try:
    import diskcache as _diskcache  # avoid shadowing confusion

    diskcache = _diskcache  # bind the runtime name
    _HAS_DISKCACHE = True
except ImportError:
    diskcache = None  # type: ignore[assignment]

# ────────────────────────────────────────────────────────────────────────────────
# Sync Session
# ────────────────────────────────────────────────────────────────────────────────


class DiskCacheSyncSession(CacheSession):
    """Synchronous disk-backed session using `diskcache.Cache | diskcache.FanoutCache`.

    - Keys are namespaced.
    - Values are stored as EncodedT (bytes-like) via `safe_encode`.
    - Expiration is handled by DiskCache per-item `expire` seconds.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    cache: diskcache.Cache | diskcache.FanoutCache = Field(..., exclude=True)

    def _get_with_expire_time(self, k: str):
        # Returns `None` if missing, else a tuple (value, expire_time)
        _missing = object()
        res = self.cache.get(k, default=_missing, expire_time=True)
        return None if res is _missing else res  # type: ignore[return-value]

    @override
    def get(self, key: str) -> DecodedT:
        k = self.namespace.apply(key)
        try:
            res = self._get_with_expire_time(k)
            if res is None:
                raise KeyError(f"No data found for key `{k}`")
            value, expire_time = res
            # If value is present but already expired by race, DiskCache returns default.
            return safe_decode(value)
        except KeyError:
            raise
        except Exception as e:
            raise GenericCacheReadError(e) from e

    @override
    def set(self, key: str, value, expiry: int | None = None) -> bool:
        k = self.namespace.apply(key)
        try:
            return bool(self.cache.set(k, safe_encode(value), expire=expiry))
        except Exception as e:
            raise GenericCacheWriteError(e) from e

    @override
    def set_if_not_exists(self, key: str, value, expiry: int | None = None) -> bool:
        k = self.namespace.apply(key)
        try:
            # `add` stores only if missing
            return bool(self.cache.add(k, safe_encode(value), expire=expiry))
        except Exception as e:
            raise GenericCacheWriteError(e) from e

    @override
    def delete(self, key: str) -> int:
        k = self.namespace.apply(key)
        try:
            return 1 if self.cache.delete(k) else 0
        except Exception as e:
            raise GenericCacheWriteError(e) from e

    @override
    def increment(
        self,
        key: str,
        *,
        increment_by: int = 1,
        initial_value: int | None = None,
        expiry: int | None = None,
    ) -> int:
        k = self.namespace.apply(key)
        try:
            # mirror in-memory semantics (store integer as utf-8 bytes)
            with self.cache.transact():
                res = self._get_with_expire_time(k)
                if res is None:
                    new_val = int(initial_value or 0)
                    self.cache.set(k, str(new_val).encode("utf-8"), expire=expiry)
                    return new_val
                value, _ = res
                try:
                    current = int(safe_decode(value))
                except Exception as conv:
                    raise GenericCacheWriteError(f"Value for key `{k}` is not an integer") from conv
                new_val = current + int(increment_by)
                # do NOT change expiry for existing key (parity with Redis impl)
                self.cache.set(k, str(new_val).encode("utf-8"))
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
            # iterkeys() may include expired; verify liveness via get(...)
            for k in self.cache.iterkeys():
                if not isinstance(k, str):
                    continue
                if not fnmatch.fnmatch(k, pat):
                    continue
                res = self._get_with_expire_time(k)
                if res is None:
                    continue  # missing/expired
                out.append(self.namespace.strip(k))
            return out
        except Exception as e:
            raise GenericCacheReadError(e) from e

    @override
    def delete_keys(self, pattern: str = "*") -> int:
        pat = self.namespace.apply(pattern)
        try:
            deleted = 0
            for k in list(self.cache.iterkeys()):
                if isinstance(k, str) and fnmatch.fnmatch(k, pat):
                    if self.cache.delete(k):
                        deleted += 1
            return deleted
        except Exception as e:
            raise GenericCacheWriteError(e) from e

    @override
    def get_ttl(self, key: str) -> int:
        k = self.namespace.apply(key)
        try:
            res = self._get_with_expire_time(k)
            if res is None:
                raise KeyError(f"No data found for key `{k}`")
            _, exp = res
            if exp is None:
                return -1  # no expiration
            ttl = int(exp - time.time())
            if ttl < 0:
                # already expired in-between; treat as missing now
                self.cache.delete(k)
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
            return bool(self.cache.touch(k, expire=int(ttl)))
        except Exception as e:
            raise GenericCacheWriteError(e) from e

    @override
    def close(self) -> None:
        try:
            self.cache.close()
        except Exception:
            pass


# ────────────────────────────────────────────────────────────────────────────────
# Async Session (wrap sync DiskCache with to_thread)
# ────────────────────────────────────────────────────────────────────────────────


class DiskCacheAsyncSession(CacheAsyncSession):
    """Async facade for `diskcache.Cache | diskcache.FanoutCache` wrapped with `asyncio.to_thread`."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    cache: diskcache.Cache | diskcache.FanoutCache = Field(..., exclude=True)

    def _ns(self, key: str) -> str:
        return self.namespace.apply(key)

    @override
    async def get(self, key: str) -> DecodedT:
        k = self._ns(key)

        def _work():
            _missing = object()
            res = self.cache.get(k, default=_missing, expire_time=True)
            if res is _missing:
                raise KeyError(f"No data found for key `{k}`")
            value, _ = res
            return safe_decode(value)

        try:
            return await asyncio.to_thread(_work)
        except KeyError:
            raise
        except Exception as e:
            raise GenericCacheReadError(e) from e

    @override
    async def set(self, key: str, value, expiry: int | None = None) -> bool:
        k = self._ns(key)
        try:
            return await asyncio.to_thread(self.cache.set, k, safe_encode(value), expiry)
        except Exception as e:
            raise GenericCacheWriteError(e) from e

    @override
    async def set_if_not_exists(self, key: str, value, expiry: int | None = None) -> bool:
        k = self._ns(key)
        try:
            return await asyncio.to_thread(self.cache.add, k, safe_encode(value), expiry)
        except Exception as e:
            raise GenericCacheWriteError(e) from e

    @override
    async def delete(self, key: str) -> int:
        k = self._ns(key)
        try:
            ok = await asyncio.to_thread(self.cache.delete, k)
            return 1 if ok else 0
        except Exception as e:
            raise GenericCacheWriteError(e) from e

    @override
    async def increment(
        self,
        key: str,
        *,
        increment_by: int = 1,
        initial_value: int | None = None,
        expiry: int | None = None,
    ) -> int:
        k = self._ns(key)

        def _work() -> int:
            with self.cache.transact():
                _missing = object()
                res = self.cache.get(k, default=_missing, expire_time=True)
                if res is _missing:
                    new_val = int(initial_value or 0)
                    self.cache.set(k, str(new_val).encode("utf-8"), expire=expiry)
                    return new_val
                value, _ = res
                try:
                    current = int(safe_decode(value))
                except Exception as conv:
                    raise GenericCacheWriteError(f"Value for key `{k}` is not an integer") from conv
                new_val = current + int(increment_by)
                self.cache.set(k, str(new_val).encode("utf-8"))
                return new_val

        try:
            return await asyncio.to_thread(_work)
        except GenericCacheWriteError:
            raise
        except Exception as e:
            raise GenericCacheWriteError(e) from e

    @override
    async def get_keys(self, pattern: str = "*") -> list[str]:
        pat = self.namespace.apply(pattern)

        def _work() -> list[str]:
            out: list[str] = []
            for k in self.cache.iterkeys():
                if not isinstance(k, str):
                    continue
                if not fnmatch.fnmatch(k, pat):
                    continue
                _missing = object()
                res = self.cache.get(k, default=_missing, expire_time=True)
                if res is _missing:
                    continue
                out.append(self.namespace.strip(k))
            return out

        try:
            return await asyncio.to_thread(_work)
        except Exception as e:
            raise GenericCacheReadError(e) from e

    @override
    async def delete_keys(self, pattern: str = "*") -> int:
        pat = self.namespace.apply(pattern)

        def _work() -> int:
            deleted = 0
            for k in list(self.cache.iterkeys()):
                if isinstance(k, str) and fnmatch.fnmatch(k, pat):
                    if self.cache.delete(k):
                        deleted += 1
            return deleted

        try:
            return await asyncio.to_thread(_work)
        except Exception as e:
            raise GenericCacheWriteError(e) from e

    @override
    async def get_ttl(self, key: str) -> int:
        k = self._ns(key)

        def _work() -> int:
            _missing = object()
            res = self.cache.get(k, default=_missing, expire_time=True)
            if res is _missing:
                raise KeyError(f"No data found for key `{k}`")
            _, exp = res
            if exp is None:
                return -1
            ttl = int(exp - time.time())
            if ttl < 0:
                self.cache.delete(k)
                raise KeyError(f"No data found for key `{k}`")
            return ttl

        try:
            return await asyncio.to_thread(_work)
        except KeyError:
            raise
        except Exception as e:
            raise GenericCacheReadError(e) from e

    @override
    async def expire(self, key: str, ttl: int) -> bool:
        k = self._ns(key)
        try:
            return await asyncio.to_thread(self.cache.touch, k, int(ttl))
        except Exception as e:
            raise GenericCacheWriteError(e) from e

    @override
    async def close(self) -> None:
        try:
            await asyncio.to_thread(self.cache.close)
        except Exception:
            pass


# ────────────────────────────────────────────────────────────────────────────────
# Cache factory
# ────────────────────────────────────────────────────────────────────────────────


class DiskCache(CacheBase[DiskCacheSyncSession, DiskCacheAsyncSession]):
    # Adjust enum name if your CacheType differs, e.g. DISKCACHE or DISK_CACHE
    type: Literal[CacheType.DISK] = CacheType.DISK

    # Where to persist on disk (e.g. a container volume path)
    directory: Path = Field(..., description="Directory for diskcache storage")
    timeout: int = Field(default=60, description="Timeout for path cache")

    # Fanout options
    fanout: bool = Field(False, description="Use diskcache.FanoutCache instead of Cache")
    shards: int = Field(8, description="Number of shards when using FanoutCache")

    @model_validator(mode="after")
    def _validate_imports(self) -> "DiskCache":
        if not _HAS_DISKCACHE:
            raise ImportError(
                "Diskcache  not installed. "
                "Install extras depending on your usage:\n"
                '  - Sync only:  pip install "ab-cache[diskcache-sync]"\n'
                '  - Async only: pip install "ab-cache[diskcache-async]"\n'
                '  - Both:       pip install "ab-cache[diskcache-sync,diskcache-async]"'
            )
        return self

    def _new_cache(self) -> diskcache.Cache | diskcache.FanoutCache:
        if self.fanout:
            return diskcache.FanoutCache(self.directory, timeout=self.timeout, shards=self.shards)
        return diskcache.Cache(self.directory, timeout=self.timeout)

    @override
    @contextmanager
    def sync_session(
        self,
        *,
        current_session: DiskCacheSyncSession | None = None,
    ) -> Iterator[DiskCacheSyncSession]:
        if current_session:
            yield current_session
        else:
            with DiskCacheSyncSession(namespace=self.namespace, cache=self._new_cache()) as session:
                yield session

    @override
    @asynccontextmanager
    async def async_session(
        self,
        *,
        current_session: DiskCacheAsyncSession | None = None,
    ) -> AsyncIterator[DiskCacheAsyncSession]:
        if current_session:
            yield current_session
        else:
            async with DiskCacheAsyncSession(namespace=self.namespace, cache=self._new_cache()) as session:
                yield session
