"""Application configuration, loaded from environment variables (prefix ``FF_``)."""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="FF_", env_file=".env", extra="ignore")

    environment: str = "local"
    # Maximum steps to run in parallel within a dependency layer.
    max_parallel: int = 4


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
