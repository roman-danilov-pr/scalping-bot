import os
from functools import lru_cache
from pydantic import Field, validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Основные настройки
    symbols: list[str] = Field(default=["BTCUSDT", "ETHUSDT"])
    window: int = Field(default=60, description="Размер окна для анализа сигналов")
    paper_mode: bool = Field(default=True, description="Режим paper trading")
    leverage: float = Field(default=10.0, description="Плечо по умолчанию")

    # Баланс для paper/trading
    initial_balance: float = Field(default=1000.0, description="Начальный баланс для торговли")

    # Ограничения риска
    max_risk_per_trade: float = Field(default=0.01, description="Максимальный риск на сделку (доля от баланса)")
    max_daily_loss_pct: float = Field(default=0.05,
                                      description="Максимальный дневной убыток, после которого стратегия приостанавливается")

    # Risk Manager
    max_risk_pct: float = Field(default=1.0, description="Макс риск на сделку (%)")
    max_daily_loss_pct: float = Field(default=5.0, description="Максимальная просадка за день (%)")
    max_position_value_pct: float = Field(default=20.0, description="Максимальная экспозиция от депозита (%)")
    commission_pct: float = Field(default=0.04, description="Комиссия (%)")
    slippage_pct: float = Field(default=0.02, description="Проскальзывание (%)")

    # API
    bybit_api_key: str = Field(default_factory=lambda: os.getenv("BYBIT_API_KEY", ""))
    bybit_api_secret: str = Field(default_factory=lambda: os.getenv("BYBIT_API_SECRET", ""))

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

    @validator("symbols", pre=True)
    def split_symbols(cls, v):
        if isinstance(v, str):
            return [s.strip() for s in v.split(",")]
        return v


@lru_cache()
def get_config() -> Settings:
    return Settings()
