from enum import StrEnum


class CacheType(StrEnum):
    REDIS = "REDIS"
    DISK = "DISK"
    INMEMORY = "INMEMORY"
    TEMPLATE = "TEMPLATE"
