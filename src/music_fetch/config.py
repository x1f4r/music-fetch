from __future__ import annotations

import json
import locale
import os
from pathlib import Path

from platformdirs import PlatformDirs
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from .models import ProviderName


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MUSIC_FETCH_", extra="ignore")

    app_name: str = "music-fetch"
    default_host: str = "127.0.0.1"
    default_port: int = 7766
    api_token: str | None = None
    base_dir: str | None = None
    max_workers: int = Field(default_factory=lambda: min(4, max(2, os.cpu_count() or 2)))
    provider_min_interval_ms: int = 350
    retain_artifacts: bool = False
    separation_model: str | None = None
    vibra_binary: str = "vibra"
    audio_separator_binary: str = "audio-separator"
    fpcalc_binary: str = "fpcalc"
    provider_order: list[ProviderName] = Field(
        default_factory=lambda: [
            ProviderName.LOCAL_CATALOG,
            ProviderName.VIBRA,
            ProviderName.AUDD,
            ProviderName.ACRCLOUD,
        ]
    )

    @property
    def dirs(self) -> PlatformDirs:
        return PlatformDirs(self.app_name, appauthor=False)

    @property
    def data_dir(self) -> Path:
        if self.base_dir:
            path = Path(self.base_dir) / "data"
            path.mkdir(parents=True, exist_ok=True)
            return path
        path = Path(self.dirs.user_data_dir)
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def cache_dir(self) -> Path:
        if self.base_dir:
            path = Path(self.base_dir) / "cache"
            path.mkdir(parents=True, exist_ok=True)
            return path
        path = Path(self.dirs.user_cache_dir)
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def config_dir(self) -> Path:
        if self.base_dir:
            path = Path(self.base_dir) / "config"
            path.mkdir(parents=True, exist_ok=True)
            return path
        path = Path(self.dirs.user_config_dir)
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def db_path(self) -> Path:
        return self.data_dir / "music_fetch.sqlite3"

    @property
    def config_path(self) -> Path:
        return self.config_dir / "config.json"


def load_user_config(settings: Settings) -> dict:
    if not settings.config_path.exists():
        return {}
    return json.loads(settings.config_path.read_text())


def save_user_config(settings: Settings, data: dict) -> None:
    settings.config_path.write_text(json.dumps(data, indent=2, sort_keys=True))


def default_ui_language() -> str:
    for key in ("LC_ALL", "LANGUAGE", "LANG"):
        value = os.environ.get(key, "").lower()
        if value.startswith("de"):
            return "de"
        if value.startswith("es"):
            return "es"
        if value.startswith("fr"):
            return "fr"
        if value.startswith("en"):
            return "en"

    locale_name = (locale.getlocale()[0] or "").lower()
    if locale_name.startswith("de"):
        return "de"
    if locale_name.startswith("es"):
        return "es"
    if locale_name.startswith("fr"):
        return "fr"
    return "en"
