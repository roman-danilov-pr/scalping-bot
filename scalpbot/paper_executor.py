# scalpbot/paper_executor.py
"""
PaperExecutor: симулирует исполнение ордеров — заполняет Market ордера моментально
по последней цене (из feed._last_price если доступно) с учётом проскальзывания и комиссии.

Возвращает dict:
{
    "status":"filled",
    "price": executed_price,
    "filled_qty": qty,
    "commission": commission_amount,
    "slippage": slippage_amount
}
"""

import random
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger("scalpbot.paper_executor")


class PaperExecutor:
    def __init__(self, feed=None, commission_rate: float = 0.0006, slippage_rate: float = 0.0005, event_sink=None):
        """
        feed: объект, из которого можно взять last price (feed._last_price[symbol]) — optional.
        commission_rate: fraction (e.g. 0.0006 => 0.06%) applied to notional (price * qty).
        slippage_rate: baseline slippage fraction; can be scaled with qty if desired.
        event_sink: optional callable to receive execution events.
        """
        self.feed = feed
        self.commission_rate = float(commission_rate)
        self.slippage_rate = float(slippage_rate)
        self.event_sink = event_sink

    async def execute_order(self, symbol: str, side: str, qty: float, price: Optional[float] = None,
                            order_type: str = "Market", context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        # determine market price
        market_price = None
        if price:
            market_price = float(price)
        elif getattr(self.feed, "_last_price", None) and symbol in getattr(self.feed, "_last_price"):
            market_price = float(self.feed._last_price.get(symbol) or 0.0)
        else:
            # fallback random small price to avoid zero
            market_price = float(random.uniform(1.0, 100.0))

        # slippage model: base slippage scaled by (qty / 0.01) capped
        slippage_factor = self.slippage_rate * max(1.0, qty / 0.01)
        # add a tiny noise
        slippage_noise = random.uniform(-slippage_factor * 0.2, slippage_factor * 0.2)
        total_slippage = max(0.0, slippage_factor + slippage_noise)

        if side.lower().startswith("b"):
            executed_price = market_price * (1.0 + total_slippage)
        else:
            executed_price = market_price * (1.0 - total_slippage)

        notional = executed_price * qty
        commission = notional * self.commission_rate  # one-sided commission for this fill

        res = {
            "status": "filled",
            "price": executed_price,
            "filled_qty": qty,
            "commission": commission,
            "slippage": total_slippage,
            "order_type": order_type,
        }

        # emit event to sink if present
        if self.event_sink:
            try:
                evt = {
                    "type": "execution",
                    "symbol": symbol,
                    "side": side,
                    "qty": qty,
                    "executed_price": executed_price,
                    "commission": commission,
                    "slippage": total_slippage,
                }
                if callable(self.event_sink):
                    maybe = self.event_sink(evt)
                    # support async sinks
                    if hasattr(maybe, "__await__"):
                        await maybe
            except Exception as e:
                logger.debug(f"event_sink error in executor: {e}")

        logger.info(f"[PaperExecutor] Filled {side} {symbol} {qty} @ {executed_price:.8f} (comm={commission:.6f})")
        return res
