from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, AsyncIterator, Iterator, Literal, Optional, override

from pydantic import ConfigDict, Field, model_validator

from ab_core.cache.codec import DecodedT, safe_decode, safe_encode
from ab_core.cache.exceptions import (
    GenericCacheReadError,
    GenericCacheWriteError,
)

from ..schema.cache_type import CacheType
from .base import CacheAsyncSession, CacheBase, CacheSession

# ---------- Optional imports & typing ----------
if TYPE_CHECKING:
    from redis import Redis as SyncRedisClient
    from redis.asyncio import Redis as AsyncRedisClient
    from redis.asyncio.cluster import RedisCluster as AsyncRedisClusterClient
    from redis.cluster import RedisCluster as SyncRedisClusterClient
else:
    SyncRedisClient = Any
    SyncRedisClusterClient = Any
    AsyncRedisClient = Any
    AsyncRedisClusterClient = Any

# Probe availability at runtime (no hard import errors here)
try:  # sync
    import redis as _redis  # type: ignore

    _HAS_REDIS_SYNC = True
except Exception:
    _redis = None  # type: ignore
    _HAS_REDIS_SYNC = False

try:  # async
    import redis.asyncio as _aredis  # type: ignore

    _HAS_REDIS_ASYNC = True
except Exception:
    _aredis = None  # type: ignore
    _HAS_REDIS_ASYNC = False


class RedisCacheSession(CacheSession):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    client: SyncRedisClient | SyncRedisClusterClient = Field(
        ...,
        exclude=True,
    )

    @override
    def get(self, key: str) -> DecodedT:
        k = self.namespace.apply(key)
        try:
            raw = self.client.get(k)
        except Exception as e:
            raise GenericCacheReadError(e) from e
        if raw is None:
            raise KeyError(f"No data found for key `{k}`")
        return safe_decode(raw)

    @override
    def set(self, key: str, value, expiry: Optional[int] = None) -> bool:
        k = self.namespace.apply(key)
        try:
            ok = self.client.set(k, safe_encode(value), ex=expiry)
            return bool(ok)
        except Exception as e:
            raise GenericCacheWriteError(e) from e

    @override
    def set_if_not_exists(self, key: str, value, expiry: Optional[int] = None) -> bool:
        k = self.namespace.apply(key)
        try:
            ok = self.client.set(k, safe_encode(value), nx=True, ex=expiry)
            return bool(ok)
        except Exception as e:
            raise GenericCacheWriteError(e) from e

    @override
    def delete(self, key: str) -> int:
        k = self.namespace.apply(key)
        try:
            return int(self.client.delete(k))
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
        script = """
        local current = redis.call('GET', KEYS[1])
        if not current then
            redis.call('SET', KEYS[1], ARGV[1])
            if ARGV[2] ~= '' then
                redis.call('EXPIRE', KEYS[1], ARGV[2])
            end
            return ARGV[1]
        else
            local new_value = redis.call('INCRBY', KEYS[1], ARGV[3])
            return new_value
        end
        """
        initial = str(initial_value if initial_value is not None else 0)
        exp = str(int(expiry)) if expiry is not None else ""
        inc = str(increment_by)
        try:
            new_val = self.client.eval(script, 1, k, initial, exp, inc)
            return int(new_val)
        except Exception as e:
            raise GenericCacheWriteError(e) from e

    @override
    def get_keys(self, pattern: str = "*") -> list[str]:
        pat = self.namespace.apply(pattern)
        try:
            keys = self.client.keys(pat)
        except Exception as e:
            raise GenericCacheReadError(e) from e
        return [self.namespace.strip(k.decode("utf-8")) for k in keys]

    @override
    def delete_keys(self, pattern: str = "*") -> int:
        pat = self.namespace.apply(pattern)
        try:
            ks = self.client.keys(pat)
            return int(self.client.delete(*ks)) if ks else 0
        except Exception as e:
            raise GenericCacheWriteError(e) from e

    @override
    def get_ttl(self, key: str) -> int:
        k = self.namespace.apply(key)
        try:
            return int(self.client.ttl(k))
        except Exception as e:
            raise GenericCacheReadError(e) from e

    @override
    def expire(self, key: str, ttl: int) -> bool:
        k = self.namespace.apply(key)
        try:
            return bool(self.client.expire(k, ttl))
        except Exception as e:
            raise GenericCacheWriteError(e) from e

    @override
    def close(self) -> None:
        try:
            self.client.close()
        except Exception:
            pass

        try:
            # ensure pool is freed for long-running apps
            self.client.connection_pool.disconnect()
        except Exception:
            pass


class RedisCacheAsyncSession(CacheAsyncSession):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    client: AsyncRedisClient | AsyncRedisClusterClient = Field(
        ...,
        exclude=True,
    )

    @override
    async def get(self, key: str) -> DecodedT:
        k = self.namespace.apply(key)
        try:
            raw = await self.client.get(k)
        except Exception as e:
            raise GenericCacheReadError(e) from e
        if raw is None:
            raise KeyError(f"No data found for key `{k}`")
        return safe_decode(raw)

    @override
    async def set(self, key: str, value, expiry: Optional[int] = None) -> bool:
        k = self.namespace.apply(key)
        try:
            ok = await self.client.set(k, safe_encode(value), ex=expiry)
            return bool(ok)
        except Exception as e:
            raise GenericCacheWriteError(e) from e

    @override
    async def set_if_not_exists(self, key: str, value, expiry: Optional[int] = None) -> bool:
        k = self.namespace.apply(key)
        try:
            ok = await self.client.set(k, safe_encode(value), nx=True, ex=expiry)
            return bool(ok)
        except Exception as e:
            raise GenericCacheWriteError(e) from e

    @override
    async def delete(self, key: str) -> int:
        k = self.namespace.apply(key)
        try:
            return int(await self.client.delete(k))
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
        script = """
        local current = redis.call('GET', KEYS[1])
        if not current then
            redis.call('SET', KEYS[1], ARGV[1])
            if ARGV[2] ~= '' then
                redis.call('EXPIRE', KEYS[1], ARGV[2])
            end
            return ARGV[1]
        else
            local new_value = redis.call('INCRBY', KEYS[1], ARGV[3])
            return new_value
        end
        """
        initial = str(initial_value if initial_value is not None else 0)
        exp = str(int(expiry)) if expiry is not None else ""
        inc = str(increment_by)
        try:
            new_val = await self.client.eval(script, 1, k, initial, exp, inc)
            return int(new_val)
        except Exception as e:
            raise GenericCacheWriteError(e) from e

    @override
    async def get_keys(self, pattern: str = "*") -> list[str]:
        pat = self.namespace.apply(pattern)
        try:
            keys = await self.client.keys(pat)
        except Exception as e:
            raise GenericCacheReadError(e) from e
        return [self.namespace.strip(k.decode("utf-8")) for k in keys]

    @override
    async def delete_keys(self, pattern: str = "*") -> int:
        pat = self.namespace.apply(pattern)
        try:
            ks = await self.client.keys(pat)
            return int(await self.client.delete(*ks)) if ks else 0
        except Exception as e:
            raise GenericCacheWriteError(e) from e

    @override
    async def get_ttl(self, key: str) -> int:
        k = self.namespace.apply(key)
        try:
            return int(await self.client.ttl(k))
        except Exception as e:
            raise GenericCacheReadError(e) from e

    @override
    async def expire(self, key: str, ttl: int) -> bool:
        k = self.namespace.apply(key)
        try:
            return bool(await self.client.expire(k, ttl))
        except Exception as e:
            raise GenericCacheWriteError(e) from e

    @override
    async def close(self) -> None:
        try:
            await self.client.aclose()
        except Exception:
            pass

        try:
            await self.client.connection_pool.disconnect()
        except Exception:
            pass


class RedisCache(CacheBase[RedisCacheSession, RedisCacheAsyncSession]):
    type: Literal[CacheType.REDIS] = CacheType.REDIS

    redis_url: str
    username: Optional[str] = None
    password: Optional[str] = None
    cluster: bool = Field(False, description="Use Redis Cluster")

    # Validate imports once the model is created
    @model_validator(mode="after")
    def _validate_imports(self) -> "RedisCache":
        if not (_HAS_REDIS_SYNC or _HAS_REDIS_ASYNC):
            raise ImportError(
                "Redis client not installed. "
                "Install extras depending on your usage:\n"
                '  - Sync only:  pip install "ab-cache[redis-sync]"\n'
                '  - Async only: pip install "ab-cache[redis-async]"\n'
                '  - Both:       pip install "ab-cache[redis-sync,redis-async]"'
            )
        return self

    def create_sync_client(self) -> SyncRedisClient | SyncRedisClusterClient:
        """Synchronous client (standalone or cluster)."""
        if self.cluster:
            return SyncRedisClusterClient.from_url(
                self.redis_url,
                username=self.username,
                password=self.password,
            )
        return SyncRedisClient.from_url(
            self.redis_url,
            username=self.username,
            password=self.password,
        )

    def create_async_client(self) -> AsyncRedisClient | AsyncRedisClusterClient:
        """Async client (standalone or cluster)."""
        if self.cluster:
            return AsyncRedisClusterClient.from_url(
                self.redis_url,
                username=self.username,
                password=self.password,
            )
        return AsyncRedisClient.from_url(
            self.redis_url,
            username=self.username,
            password=self.password,
        )

    @override
    @contextmanager
    def sync_session(
        self,
        *,
        current_session: Optional[RedisCacheSession] = None,
    ) -> Iterator[RedisCacheSession]:
        if current_session:
            yield current_session
        else:
            with RedisCacheSession(
                namespace=self.namespace,
                client=self.create_sync_client(),
            ) as session:
                yield session

    @override
    @asynccontextmanager
    async def async_session(
        self,
        *,
        current_session: Optional[RedisCacheAsyncSession] = None,
    ) -> AsyncIterator[RedisCacheAsyncSession]:
        if current_session:
            yield current_session
        else:
            async with RedisCacheAsyncSession(
                namespace=self.namespace,
                client=self.create_async_client(),
            ) as session:
                yield session
