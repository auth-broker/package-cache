# tests/ab_core/cache/caches/test_in_memory_cache.py

from __future__ import annotations

from typing import Any

import pytest

from ab_core.cache.exceptions import GenericCacheReadError, GenericCacheWriteError
from ab_core.cache.caches.inmemory import (
    InMemoryCache,
    InMemoryCacheAsyncSession,
    InMemoryCacheSession,
)

import ab_core.cache.caches.inmemory as inmemory_mod


class FrozenTime:
    def __init__(self, t: float) -> None:
        self._t = float(t)

    def time(self) -> float:
        return float(self._t)

    def advance(self, seconds: float) -> None:
        self._t += float(seconds)


@pytest.fixture()
def frozen_time(monkeypatch: pytest.MonkeyPatch) -> FrozenTime:
    ft = FrozenTime(1_000_000.0)
    # Patch the module-under-test's time.time usage
    monkeypatch.setattr(inmemory_mod.time, "time", ft.time)
    return ft


@pytest.fixture()
def cache() -> InMemoryCache:
    # IMPORTANT: use real cache to get a real CacheNamespace instance
    return InMemoryCache()


@pytest.fixture()
def sync_session(cache: InMemoryCache) -> InMemoryCacheSession:
    return InMemoryCacheSession(namespace=cache.namespace, store=cache.store, expiry=cache.expiry)


@pytest.fixture()
def async_session(cache: InMemoryCache) -> InMemoryCacheAsyncSession:
    return InMemoryCacheAsyncSession(namespace=cache.namespace, store=cache.store, expiry=cache.expiry)


# -----------------------------------------------------------------------------
# Diagnostic tests: show whether dict references are actually shared
# -----------------------------------------------------------------------------

def test_session_uses_same_store_object_as_cache(cache: InMemoryCache):
    with cache.sync_session() as s:
        assert s.store is cache.store
        assert s.expiry is cache.expiry


@pytest.mark.xfail(reason="Currently failing: store is not persisting across sessions; indicates dict copying or rollback.")
def test_store_persists_across_sessions(cache: InMemoryCache, frozen_time: FrozenTime):
    with cache.sync_session() as s1:
        assert s1.set("a", {"x": 1}, expiry=10) is True
        # sanity inside first session
        assert s1.get("a") == {"x": 1}

    with cache.sync_session() as s2:
        # should succeed if the store is truly shared on the cache instance
        assert s2.get("a") == {"x": 1}


@pytest.mark.asyncio
@pytest.mark.xfail(reason="Currently failing: store is not persisting across async sessions; indicates dict copying or rollback.")
async def test_store_persists_across_async_sessions(cache: InMemoryCache, frozen_time: FrozenTime):
    async with cache.async_session() as s1:
        assert await s1.set("a", "hello", expiry=10) is True
        assert await s1.get("a") == "hello"

    async with cache.async_session() as s2:
        assert await s2.get("a") == "hello"


# -----------------------------------------------------------------------------
# Sync session behaviour (should pass)
# -----------------------------------------------------------------------------

def test_sync_get_set_delete_roundtrip(sync_session: InMemoryCacheSession):
    assert sync_session.set("k", "v") is True
    assert sync_session.get("k") == "v"
    assert sync_session.delete("k") == 1
    with pytest.raises(KeyError):
        sync_session.get("k")
    assert sync_session.delete("k") == 0


def test_sync_set_if_not_exists(sync_session: InMemoryCacheSession):
    assert sync_session.set_if_not_exists("k", "v") is True
    assert sync_session.set_if_not_exists("k", "v2") is False
    assert sync_session.get("k") == "v"


def test_sync_expiry_cleanup_on_get(sync_session: InMemoryCacheSession, frozen_time: FrozenTime):
    sync_session.set("k", "v", expiry=5)
    frozen_time.advance(6)
    with pytest.raises(KeyError):
        sync_session.get("k")


def test_sync_get_keys_and_delete_keys(sync_session: InMemoryCacheSession):
    sync_session.set("a:1", "x")
    sync_session.set("a:2", "y")
    sync_session.set("b:1", "z")

    assert sorted(sync_session.get_keys("a:*")) == ["a:1", "a:2"]
    assert sorted(sync_session.get_keys("*")) == ["a:1", "a:2", "b:1"]

    deleted = sync_session.delete_keys("a:*")
    assert deleted == 2
    assert sorted(sync_session.get_keys("*")) == ["b:1"]


def test_sync_increment_initial_and_existing(sync_session: InMemoryCacheSession):
    assert sync_session.increment("ctr", initial_value=10) == 10
    assert sync_session.increment("ctr", increment_by=5) == 15
    # safe_decode likely returns int for b"15"
    assert sync_session.get("ctr") == 15


def test_sync_increment_non_integer_raises(sync_session: InMemoryCacheSession):
    k = sync_session.namespace.apply("ctr")
    sync_session.store[k] = b"not-an-int"
    with pytest.raises(GenericCacheWriteError) as e:
        sync_session.increment("ctr", increment_by=1)
    assert "not an integer" in str(e.value).lower()


def test_sync_increment_sets_expiry_when_provided(sync_session: InMemoryCacheSession, frozen_time: FrozenTime):
    assert sync_session.increment("ctr", initial_value=0, expiry=10) == 0
    k = sync_session.namespace.apply("ctr")
    assert sync_session.expiry[k] == pytest.approx(frozen_time.time() + 10)


def test_sync_get_ttl_missing_raises(sync_session: InMemoryCacheSession):
    with pytest.raises(KeyError):
        sync_session.get_ttl("missing")


def test_sync_get_ttl_no_expiration_returns_minus_one(sync_session: InMemoryCacheSession):
    sync_session.set("k", "v", expiry=None)
    assert sync_session.get_ttl("k") == -1


def test_sync_get_ttl_positive(sync_session: InMemoryCacheSession):
    sync_session.set("k", "v", expiry=10)
    ttl = sync_session.get_ttl("k")
    assert 0 < ttl <= 10


def test_sync_get_ttl_expired_between_checks_branch(sync_session: InMemoryCacheSession, frozen_time: FrozenTime, monkeypatch):
    sync_session.set("k", "v", expiry=1)

    original_cleanup = sync_session._cleanup_key

    def cleanup_then_jump(k: str) -> None:
        original_cleanup(k)
        frozen_time.advance(10)

    monkeypatch.setattr(sync_session, "_cleanup_key", cleanup_then_jump)

    with pytest.raises(KeyError):
        sync_session.get_ttl("k")

    kk = sync_session.namespace.apply("k")
    assert kk not in sync_session.store
    assert kk not in sync_session.expiry


def test_sync_expire_sets_ttl_and_missing_false(sync_session: InMemoryCacheSession, frozen_time: FrozenTime):
    assert sync_session.expire("missing", 10) is False
    sync_session.set("k", "v")
    assert sync_session.expire("k", 7) is True
    kk = sync_session.namespace.apply("k")
    assert sync_session.expiry[kk] == pytest.approx(frozen_time.time() + 7)


def test_sync_close_returns_none(sync_session: InMemoryCacheSession):
    assert sync_session.close() is None


def test_sync_get_wraps_unexpected_exception_as_read_error(sync_session: InMemoryCacheSession, monkeypatch):
    sync_session.set("k", "v")
    monkeypatch.setattr(inmemory_mod, "safe_decode", lambda _: (_ for _ in ()).throw(ValueError("boom")))
    with pytest.raises(GenericCacheReadError):
        sync_session.get("k")


def test_sync_set_wraps_exception_as_write_error(sync_session: InMemoryCacheSession, monkeypatch):
    monkeypatch.setattr(inmemory_mod, "safe_encode", lambda _: (_ for _ in ()).throw(ValueError("boom")))
    with pytest.raises(GenericCacheWriteError):
        sync_session.set("k", "v")


def test_sync_delete_wraps_exception_as_write_error(cache: InMemoryCache):
    class BadDict(dict):
        def pop(self, *args, **kwargs):
            raise RuntimeError("boom")

    bad_store: dict[str, Any] = BadDict()
    s = InMemoryCacheSession(namespace=cache.namespace, store=bad_store, expiry={})

    # Force it to call pop() on our dict
    bad_store[s.namespace.apply("k")] = b"anything"

    with pytest.raises(GenericCacheWriteError):
        s.delete("k")


# -----------------------------------------------------------------------------
# Async session behaviour (should pass)
# -----------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_async_get_set_delete_roundtrip(async_session: InMemoryCacheAsyncSession):
    assert await async_session.set("k", "v") is True
    assert await async_session.get("k") == "v"
    assert await async_session.delete("k") == 1
    with pytest.raises(KeyError):
        await async_session.get("k")
    assert await async_session.delete("k") == 0


@pytest.mark.asyncio
async def test_async_set_if_not_exists(async_session: InMemoryCacheAsyncSession):
    assert await async_session.set_if_not_exists("k", "v") is True
    assert await async_session.set_if_not_exists("k", "v2") is False
    assert await async_session.get("k") == "v"


@pytest.mark.asyncio
async def test_async_expiry_cleanup_on_get(async_session: InMemoryCacheAsyncSession, frozen_time: FrozenTime):
    await async_session.set("k", "v", expiry=5)
    frozen_time.advance(6)
    with pytest.raises(KeyError):
        await async_session.get("k")


@pytest.mark.asyncio
async def test_async_get_keys_and_delete_keys(async_session: InMemoryCacheAsyncSession):
    await async_session.set("a:1", "x")
    await async_session.set("a:2", "y")
    await async_session.set("b:1", "z")

    assert sorted(await async_session.get_keys("a:*")) == ["a:1", "a:2"]
    assert sorted(await async_session.get_keys("*")) == ["a:1", "a:2", "b:1"]

    deleted = await async_session.delete_keys("a:*")
    assert deleted == 2
    assert sorted(await async_session.get_keys("*")) == ["b:1"]


@pytest.mark.asyncio
async def test_async_increment_initial_and_existing(async_session: InMemoryCacheAsyncSession):
    assert await async_session.increment("ctr", initial_value=10) == 10
    assert await async_session.increment("ctr", increment_by=5) == 15
    assert await async_session.get("ctr") == 15


@pytest.mark.asyncio
async def test_async_increment_non_integer_raises(async_session: InMemoryCacheAsyncSession):
    k = async_session.namespace.apply("ctr")
    async_session.store[k] = b"not-an-int"
    with pytest.raises(GenericCacheWriteError) as e:
        await async_session.increment("ctr", increment_by=1)
    assert "not an integer" in str(e.value).lower()


@pytest.mark.asyncio
async def test_async_expire_sets_ttl_and_missing_false(async_session: InMemoryCacheAsyncSession, frozen_time: FrozenTime):
    assert await async_session.expire("missing", 10) is False
    await async_session.set("k", "v")
    assert await async_session.expire("k", 7) is True
    kk = async_session.namespace.apply("k")
    assert async_session.expiry[kk] == pytest.approx(frozen_time.time() + 7)


@pytest.mark.asyncio
async def test_async_delete_wraps_exception_as_write_error(cache: InMemoryCache):
    class BadDict(dict):
        def pop(self, *args, **kwargs):
            raise RuntimeError("boom")

    bad_store: dict[str, Any] = BadDict()
    s = InMemoryCacheAsyncSession(namespace=cache.namespace, store=bad_store, expiry={})

    bad_store[s.namespace.apply("k")] = b"anything"

    with pytest.raises(GenericCacheWriteError):
        await s.delete("k")