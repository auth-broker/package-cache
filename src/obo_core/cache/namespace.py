from pydantic import BaseModel, Field, field_validator


class CacheNamespace(BaseModel):
    namespace: str | None = Field(
        default=None,
    )

    @field_validator("namespace", mode="after")
    @classmethod
    def process_namespace(cls, v: str | None) -> str | None:
        if v and not v.endswith(":"):
            v += ":"
        return v

    def apply(self, key: str) -> str:
        return f"{self.namespace}{key}" if self.namespace else key

    def strip(self, key: str) -> str:
        if self.namespace and key.startswith(self.namespace):
            return key[len(self.namespace) :]
        return key
