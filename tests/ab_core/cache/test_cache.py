import asyncio
import time

import pytest

from ab_core.cache.caches import Cache  # keep your existing fixture type


def test_cache_sync(tmp_cache_sync: Cache):
    # basic set/get and set_if_not_exists
    with tmp_cache_sync.sync_session() as s:
        assert s.delete("missing") in (0, 1)  # deleting non-existing is 0 for in-memory/redis
        assert s.set("a", {"name": "Alice"}) is True
        assert s.get("a") == {"name": "Alice"}

        assert s.set_if_not_exists("a", "x") is False
        assert s.set_if_not_exists("b", "Bee") is True
        assert s.get("b") == "Bee"

    # increment semantics:
    # - first call with missing key returns initial_value (defaults to 0)
    # - subsequent calls return incremented value
    with tmp_cache_sync.sync_session() as s:
        assert s.increment("cnt") == 0  # created with initial 0
        assert s.increment("cnt") == 1  # now 1
        assert s.increment("cnt", increment_by=3) == 4  # 1 + 3

    # TTL via set(expiry=...) and expire()
    with tmp_cache_sync.sync_session() as s:
        assert s.set("t", "v", expiry=1) is True
        ttl = s.get_ttl("t")
        assert ttl >= 0  # should be >=0 while alive
        time.sleep(1.1)
        with pytest.raises(KeyError):
            s.get("t")

        assert s.set("u", "v") is True
        assert s.expire("u", 1) is True
        time.sleep(1.1)
        with pytest.raises(KeyError):
            s.get("u")

    # key patterns + bulk delete
    with tmp_cache_sync.sync_session() as s:
        assert s.set("user:1", "A")
        assert s.set("user:2", "B")
        keys = sorted(s.get_keys("user:*"))
        assert keys == ["user:1", "user:2"]

        deleted = s.delete_keys("user:*")
        assert deleted >= 2
        assert s.get_keys("user:*") == []


@pytest.mark.asyncio
async def test_cache_async(tmp_cache_async: Cache):
    # basic set/get and set_if_not_exists
    async with tmp_cache_async.async_session() as s:
        assert await s.delete("missing") in (0, 1)
        assert await s.set("a", {"name": "Alice"}) is True
        assert await s.get("a") == {"name": "Alice"}

        assert await s.set_if_not_exists("a", "x") is False
        assert await s.set_if_not_exists("b", "Bee") is True
        assert await s.get("b") == "Bee"

    # increment
    async with tmp_cache_async.async_session() as s:
        assert await s.increment("cnt") == 0
        assert await s.increment("cnt") == 1
        assert await s.increment("cnt", increment_by=3) == 4

    # TTL via set(expiry=...) and expire()
    async with tmp_cache_async.async_session() as s:
        assert await s.set("t", "v", expiry=1) is True
        ttl = await s.get_ttl("t")
        assert ttl >= 0
        await asyncio.sleep(1.1)
        with pytest.raises(KeyError):
            await s.get("t")

        assert await s.set("u", "v") is True
        assert await s.expire("u", 1) is True
        await asyncio.sleep(1.1)
        with pytest.raises(KeyError):
            await s.get("u")

    # key patterns + bulk delete
    async with tmp_cache_async.async_session() as s:
        assert await s.set("user:1", "A")
        assert await s.set("user:2", "B")
        keys = sorted(await s.get_keys("user:*"))
        assert keys == ["user:1", "user:2"]

        deleted = await s.delete_keys("user:*")
        assert deleted >= 2
        assert await s.get_keys("user:*") == []
