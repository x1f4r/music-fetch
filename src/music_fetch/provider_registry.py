from __future__ import annotations

from .config import Settings
from .db import Database
from .models import ProviderConfig, ProviderName, ProviderState
from .providers import ACRCloudProvider, AudDProvider, LocalCatalogProvider, VibraProvider
from .providers.base import BaseProvider


class ProviderRegistry:
    def __init__(self, settings: Settings, db: Database) -> None:
        self.settings = settings
        self.db = db

    def active_providers(self) -> list[BaseProvider]:
        return self.active_providers_for_order()

    def active_providers_for_order(self, order: list[ProviderName] | None = None) -> list[BaseProvider]:
        saved = self.db.get_provider_configs()
        provider_chain = self._provider_chain(saved)
        active: list[BaseProvider] = []
        provider_order = order or self.settings.provider_order
        for name in provider_order:
            provider = provider_chain[name]
            config = saved.get(name)
            if config and not config.enabled:
                continue
            active.append(provider)
        return active

    def provider_states(self) -> list[ProviderState]:
        saved = self.db.get_provider_configs()
        states: list[ProviderState] = []
        for provider in self._provider_chain(saved).values():
            state = provider.state()
            config = saved.get(state.name)
            if config:
                state = state.model_copy(update={"enabled": config.enabled, "config": {**state.config, **config.config}})
            states.append(state)
        order = {name: index for index, name in enumerate(self.settings.provider_order)}
        states.sort(key=lambda state: order.get(state.name, 999))
        return states

    def set_provider_config(self, name: ProviderName, config: ProviderConfig) -> ProviderState:
        self.db.set_provider_config(name, config)
        return {state.name: state for state in self.provider_states()}[name]

    def _provider_chain(self, saved: dict[ProviderName, ProviderConfig]) -> dict[ProviderName, BaseProvider]:
        return {
            ProviderName.LOCAL_CATALOG: LocalCatalogProvider(self.settings, self.db),
            ProviderName.VIBRA: VibraProvider(self.settings),
            ProviderName.AUDD: AudDProvider((saved.get(ProviderName.AUDD) or ProviderConfig()).config.get("api_token")),
            ProviderName.ACRCLOUD: ACRCloudProvider(
                (saved.get(ProviderName.ACRCLOUD) or ProviderConfig()).config.get("host"),
                (saved.get(ProviderName.ACRCLOUD) or ProviderConfig()).config.get("access_key"),
                (saved.get(ProviderName.ACRCLOUD) or ProviderConfig()).config.get("access_secret"),
            ),
        }
