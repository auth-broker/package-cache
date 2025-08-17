from abc import ABC, abstractmethod
from typing import (
    Any,
    AsyncContextManager,
    ContextManager,
    Generic,
    Optional,
    Self,
    TypeVar,
)

from pydantic import BaseModel, Field

from ab_core.cache.namespace import CacheNamespace


class CacheSession(BaseModel, ABC):
    namespace: CacheNamespace = Field(
        default_factory=CacheNamespace,
    )

    @abstractmethod
    def get(self, key: str): ...

    @abstractmethod
    def set(self, key: str, value, expiry: Optional[int] = None) -> bool: ...

    @abstractmethod
    def set_if_not_exists(self, key: str, value, expiry: Optional[int] = None) -> bool: ...

    @abstractmethod
    def delete(self, key: str) -> int: ...

    @abstractmethod
    def increment(
        self,
        key: str,
        *,
        increment_by: int = 1,
        initial_value: Optional[int] = None,
        expiry: Optional[int] = None,
    ) -> int: ...

    @abstractmethod
    def get_keys(self, pattern: str = "*"): ...

    @abstractmethod
    def delete_keys(self, pattern: str = "*") -> int: ...

    @abstractmethod
    def get_ttl(self, key: str) -> int: ...

    @abstractmethod
    def expire(self, key: str, ttl: int) -> bool: ...

    @abstractmethod
    def close(self) -> None: ...

    def __enter__(self: Self) -> Self:
        return self

    def __exit__(self, type_: Any, value: Any, traceback: Any) -> None:
        self.close()


class CacheAsyncSession(BaseModel, ABC):
    namespace: CacheNamespace = Field(
        default_factory=CacheNamespace,
    )

    @abstractmethod
    async def get(self, key: str): ...

    @abstractmethod
    async def set(self, key: str, value, expiry: Optional[int] = None) -> bool: ...

    @abstractmethod
    async def set_if_not_exists(self, key: str, value, expiry: Optional[int] = None) -> bool: ...

    @abstractmethod
    async def delete(self, key: str) -> int: ...

    @abstractmethod
    async def increment(
        self,
        key: str,
        *,
        increment_by: int = 1,
        initial_value: Optional[int] = None,
        expiry: Optional[int] = None,
    ) -> int: ...

    @abstractmethod
    async def get_keys(self, pattern: str = "*"): ...

    @abstractmethod
    async def delete_keys(self, pattern: str = "*") -> int: ...

    @abstractmethod
    async def get_ttl(self, key: str) -> int: ...

    @abstractmethod
    async def expire(self, key: str, ttl: int) -> bool: ...

    @abstractmethod
    async def close(self) -> None: ...

    async def __aenter__(self: Self) -> Self:
        return self

    async def __aexit__(self, type_: Any, value: Any, traceback: Any) -> None:
        await self.close()


SYNC_SESSION = TypeVar("SYNC_SESSION", bound=CacheSession)
ASYNC_SESSION = TypeVar("ASYNC_SESSION", bound=CacheAsyncSession)


class CacheBase(BaseModel, Generic[SYNC_SESSION, ASYNC_SESSION], ABC):
    namespace: CacheNamespace = Field(default_factory=CacheNamespace)

    @abstractmethod
    def sync_session(
        self,
        *,
        current_session: Optional[SYNC_SESSION] = None,
    ) -> ContextManager[SYNC_SESSION]: ...

    @abstractmethod
    async def async_session(
        self,
        *,
        current_session: Optional[ASYNC_SESSION] = None,
    ) -> AsyncContextManager[ASYNC_SESSION]: ...
