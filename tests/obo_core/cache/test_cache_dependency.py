import os
from typing import Annotated
from unittest.mock import patch

from pydantic import BaseModel

from obo_core.cache.caches import Cache, InMemoryCache
from obo_core.dependency import Depends, inject


def test_cache_dependency():
    with patch.dict(
        os.environ,
        {
            "CACHE_TYPE": "INMEMORY",
        },
        clear=False,
    ):
        # test function
        @inject
        def some_func(
            cache: Annotated[Cache, Depends(Cache, persist=True)],
        ):
            return cache

        cache1 = some_func()
        assert isinstance(cache1, InMemoryCache)

        # test class
        @inject
        class SomeClass(BaseModel):
            cache: Annotated[Cache, Depends(Cache, persist=True)]

        cache2 = SomeClass().cache
        assert isinstance(cache2, InMemoryCache)

        assert cache1 is cache2
