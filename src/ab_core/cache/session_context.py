import logging
from contextlib import asynccontextmanager, contextmanager
from typing import (
    Annotated,
)

from ab_core.dependency import Depends, inject, sentinel

from .caches import Cache

logger = logging.getLogger(__name__)


@inject
@contextmanager
def cache_session_sync_cm(
    cache: Annotated[Cache, Depends(Cache)] = sentinel(),
):
    with cache.sync_session() as sync_session:
        try:
            yield sync_session
        except Exception as e:
            logger.debug(
                "An exception occurred, unable to perform cache rollback",
                exc_info=e,
            )
            # sync_session.rollback()
            raise
        else:
            pass
            # sync_session.commit()
        finally:
            sync_session.close()


@inject
@asynccontextmanager
async def cache_session_async_cm(
    cache: Annotated[Cache, Depends(Cache)] = sentinel(),
):
    async with cache.async_session() as async_session:
        try:
            yield async_session
        except Exception as e:
            logger.debug(
                "An exception occurred, unable to perform cache rollback",
                exc_info=e,
            )
            # await async_session.rollback()
            raise
        else:
            pass
            # await async_session.commit()
        finally:
            await async_session.close()


# NOTE: below can be used as fastapi dependencies, since they don't
# have the context manager annotation


@inject
def cache_session_sync(
    cache: Annotated[Cache, Depends(Cache)] = sentinel(),
):
    with cache_session_sync_cm(cache) as sync_session:
        yield sync_session


@inject
async def cache_session_async(
    cache: Annotated[Cache, Depends(Cache)] = sentinel(),
):
    async with cache_session_async_cm(cache) as async_session:
        yield async_session
