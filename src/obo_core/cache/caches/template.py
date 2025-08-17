from contextlib import asynccontextmanager, contextmanager
from typing import AsyncIterator, Iterator, Literal, Optional, override

from ..schema.cache_type import CacheType
from .base import CacheAsyncSession, CacheBase, CacheSession


class TemplateCacheSession(CacheSession):
    """Sync session template – all methods intentionally unimplemented."""

    @override
    def get(self, key: str):
        raise NotImplementedError("TemplateSessionSync.get is not implemented")

    @override
    def set(self, key: str, value, expiry: Optional[int] = None) -> bool:
        raise NotImplementedError("TemplateSessionSync.set is not implemented")

    @override
    def set_if_not_exists(self, key: str, value, expiry: Optional[int] = None) -> bool:
        raise NotImplementedError("TemplateSessionSync.set_if_not_exists is not implemented")

    @override
    def delete(self, key: str) -> int:
        raise NotImplementedError("TemplateSessionSync.delete is not implemented")

    @override
    def increment(
        self,
        key: str,
        *,
        increment_by: int = 1,
        initial_value: Optional[int] = None,
        expiry: Optional[int] = None,
    ) -> int:
        raise NotImplementedError("TemplateSessionSync.increment is not implemented")

    @override
    def get_keys(self, pattern: str = "*"):
        raise NotImplementedError("TemplateSessionSync.get_keys is not implemented")

    @override
    def delete_keys(self, pattern: str = "*") -> int:
        raise NotImplementedError("TemplateSessionSync.delete_keys is not implemented")

    @override
    def get_ttl(self, key: str) -> int:
        raise NotImplementedError("TemplateSessionSync.get_ttl is not implemented")

    @override
    def expire(self, key: str, ttl: int) -> bool:
        raise NotImplementedError("TemplateSessionSync.expire is not implemented")

    @override
    def close(self) -> None:
        raise NotImplementedError("TemplateSessionSync.close is not implemented")


class TemplateCacheAsyncSession(CacheAsyncSession):
    """Async session template – all methods intentionally unimplemented."""

    @override
    async def get(self, key: str):
        raise NotImplementedError("TemplateSessionAsync.get is not implemented")

    @override
    async def set(self, key: str, value, expiry: Optional[int] = None) -> bool:
        raise NotImplementedError("TemplateSessionAsync.set is not implemented")

    @override
    async def set_if_not_exists(self, key: str, value, expiry: Optional[int] = None) -> bool:
        raise NotImplementedError("TemplateSessionAsync.set_if_not_exists is not implemented")

    @override
    async def delete(self, key: str) -> int:
        raise NotImplementedError("TemplateSessionAsync.delete is not implemented")

    @override
    async def increment(
        self,
        key: str,
        *,
        increment_by: int = 1,
        initial_value: Optional[int] = None,
        expiry: Optional[int] = None,
    ) -> int:
        raise NotImplementedError("TemplateSessionAsync.increment is not implemented")

    @override
    async def get_keys(self, pattern: str = "*"):
        raise NotImplementedError("TemplateSessionAsync.get_keys is not implemented")

    @override
    async def delete_keys(self, pattern: str = "*") -> int:
        raise NotImplementedError("TemplateSessionAsync.delete_keys is not implemented")

    @override
    async def get_ttl(self, key: str) -> int:
        raise NotImplementedError("TemplateSessionAsync.get_ttl is not implemented")

    @override
    async def expire(self, key: str, ttl: int) -> bool:
        raise NotImplementedError("TemplateSessionAsync.expire is not implemented")

    @override
    async def close(self) -> None:
        raise NotImplementedError("TemplateSessionAsync.aclose is not implemented")


class TemplateCache(CacheBase[TemplateCacheSession, TemplateCacheAsyncSession]):
    """Template cache - provides the same surface as RedisCache but unimplemented."""

    type: Literal[CacheType.TEMPLATE] = CacheType.TEMPLATE

    @override
    @contextmanager
    def sync_session(
        self,
        *,
        current_session: Optional[TemplateCacheSession] = None,
    ) -> Iterator[TemplateCacheSession]:
        if current_session:
            yield current_session
        else:
            with TemplateCacheSession(
                namespace=self.namespace,
            ) as session:
                yield session

    @override
    @asynccontextmanager
    async def async_session(
        self,
        *,
        current_session: Optional[TemplateCacheAsyncSession] = None,
    ) -> AsyncIterator[TemplateCacheAsyncSession]:
        if current_session:
            yield current_session
        else:
            async with TemplateCacheAsyncSession(
                namespace=self.namespace,
            ) as session:
                yield session
