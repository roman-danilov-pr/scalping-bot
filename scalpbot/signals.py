# scalpbot/signals.py
"""
Signals and feature engineering utilities for scalpbot.

This module provides a TradeAggregator to process raw trades (canonical format)
and compute features that can be used by strategies:
- rolling VWAP
- rolling buy/sell volume imbalance
- simple momentum features

All functions work with the canonical trade dict:
{
  "topic": "trade.BTCUSDT",
  "trade": {"symbol": str, "ts": int, "price": float, "vol": float, "side": "Buy"|"Sell"}
}
"""

from collections import deque
from typing import Deque, Dict, List, Optional
import numpy as np


class TradeAggregator:
    def __init__(self, window: int = 60):
        self.window = window
        self.trades: Deque[Dict] = deque(maxlen=10_000)

    def add_trade(self, trade: Dict):
        """Add a canonical trade dict."""
        if not trade or "trade" not in trade:
            return
        t = trade["trade"]
        if not all(k in t for k in ("price", "vol", "side", "symbol", "ts")):
            return
        try:
            price = float(t["price"])
            vol = float(t["vol"])
            side = str(t["side"])
        except Exception:
            return
        self.trades.append({
            "ts": int(t["ts"]),
            "price": price,
            "vol": vol,
            "side": side,
            "symbol": t["symbol"],
        })

    def get_recent(self, lookback_ms: int) -> List[Dict]:
        if not self.trades:
            return []
        cutoff = self.trades[-1]["ts"] - lookback_ms
        return [t for t in self.trades if t["ts"] >= cutoff]

    def vwap(self, lookback_ms: int) -> Optional[float]:
        recent = self.get_recent(lookback_ms)
        if not recent:
            return None
        total_vol = sum(t["vol"] for t in recent)
        if total_vol <= 0:
            return None
        vwap = sum(t["price"] * t["vol"] for t in recent) / total_vol
        return vwap

    def imbalance(self, lookback_ms: int) -> float:
        """Return buy-sell volume imbalance in [-1,1]."""
        recent = self.get_recent(lookback_ms)
        if not recent:
            return 0.0
        buy_vol = sum(t["vol"] for t in recent if t["side"].lower() == "buy")
        sell_vol = sum(t["vol"] for t in recent if t["side"].lower() == "sell")
        total = buy_vol + sell_vol
        if total <= 0:
            return 0.0
        return (buy_vol - sell_vol) / total

    def momentum(self, lookback_ms: int) -> float:
        """Simple price momentum (last - first) / first."""
        recent = self.get_recent(lookback_ms)
        if len(recent) < 2:
            return 0.0
        first = recent[0]["price"]
        last = recent[-1]["price"]
        if first <= 0:
            return 0.0
        return (last - first) / first

    def feature_vector(self, lookback_ms: int) -> Dict[str, float]:
        return {
            "vwap": self.vwap(lookback_ms) or 0.0,
            "imbalance": self.imbalance(lookback_ms),
            "momentum": self.momentum(lookback_ms),
        }
