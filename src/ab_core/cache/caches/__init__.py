from typing import Annotated, Union

from pydantic import Discriminator

from .disk import DiskCache
from .inmemory import InMemoryCache
from .redis import RedisCache
from .template import TemplateCache

Cache = Annotated[RedisCache | TemplateCache | InMemoryCache | DiskCache, Discriminator("type")]
