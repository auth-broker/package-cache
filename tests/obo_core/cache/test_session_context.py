import pytest

from obo_core.cache.caches import Cache
from obo_core.cache.session_context import cache_session_async_cm, cache_session_sync_cm


def test_cache_session_sync_persists(tmp_cache_sync: Cache):
    # write inside the CM (no explicit commit)
    with cache_session_sync_cm(cache=tmp_cache_sync) as s:
        assert s.set("user:charlie", {"name": "Charlie"}) is True
        # session is usable inside the block
        assert s.get("user:charlie") == {"name": "Charlie"}


def test_cache_session_sync_no_rollback(tmp_cache_sync: Cache):
    # since cache has no transactional semantics, data remains even if an error occurs
    with pytest.raises(RuntimeError):
        with cache_session_sync_cm(cache=tmp_cache_sync) as s:
            assert s.set("user:eve", {"name": "Eve"}) is True
            raise RuntimeError("boom")  # should NOT roll back the set


@pytest.mark.asyncio
async def test_cache_session_async_persists(tmp_cache_async: Cache):
    # write inside the async CM
    async with cache_session_async_cm(cache=tmp_cache_async) as s:
        assert await s.set("user:charlie", {"name": "Charlie"}) is True
        assert await s.get("user:charlie") == {"name": "Charlie"}


@pytest.mark.asyncio
async def test_cache_session_async_no_rollback(tmp_cache_async: Cache):
    # again, no transactional rollback: data remains after exception
    with pytest.raises(RuntimeError):
        async with cache_session_async_cm(cache=tmp_cache_async) as s:
            assert await s.set("user:eve", {"name": "Eve"}) is True
            raise RuntimeError("boom")
