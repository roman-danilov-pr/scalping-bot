# scalpbot/strategy.py
"""
Strategy using TradeAggregator from signals.py and integrated RiskManager.

Main changes:
- Strategy receives a RiskManager instance (or creates one).
- Before placing orders, quantity is calculated by RiskManager.calculate_qty_from_risk.
- On filled orders RiskManager.register_open_position is called.
- On closes RiskManager.register_close_position and register_trade_result are called.
- Strategy pauses trading until next UTC midnight if daily loss limit exceeded.
"""

import asyncio
import logging
from collections import deque
from datetime import datetime, timedelta
from typing import Deque, Dict, Any, Optional, List

from signals import TradeAggregator
from shared_state import get_state
from risk_manager import RiskManager, RiskManagerConfig

logger = logging.getLogger("scalpbot.strategy")
state = get_state()


class Strategy:
    def __init__(self, client, symbols: List[str], window: int = 60,
                 enabled_strategies: Optional[Dict[str, bool]] = None,
                 risk_manager: Optional[RiskManager] = None):
        self.client = client
        self.symbols = symbols
        # window in seconds
        self.window = window
        self.lookback_ms = int(window * 1000)

        self.enabled_strategies = enabled_strategies or {
            "volume_spike": True,
            "ema_crossover": True,
            "price_momentum": True,
            "vwap": True,
            "order_book_imbalance": True,
            "candlestick_pattern": True,
        }

        # stop coordination
        self._stop_event = asyncio.Event()

        # histories
        self.trade_history: Dict[str, Deque[Dict[str, Any]]] = {s: deque(maxlen=1000) for s in symbols}
        self.price_history: Dict[str, Deque[float]] = {s: deque(maxlen=1000) for s in symbols}
        self.vwap_history: Dict[str, Deque[Any]] = {s: deque(maxlen=1000) for s in symbols}

        # aggregators per symbol
        self.aggregators: Dict[str, TradeAggregator] = {s: TradeAggregator(window=window) for s in symbols}

        # EMA state
        self._ema_last_signal: Dict[str, Optional[str]] = {s: None for s in symbols}
        self._last_trade_time: Dict[str, datetime] = {s: datetime.min for s in symbols}
        self.trade_cooldown = timedelta(seconds=5)
        self.open_positions: Dict[str, Optional[Dict[str, Any]]] = {s: None for s in symbols}

        # performance/balance etc
        self.strategy_performance: Dict[str, Dict[str, Any]] = {s: {"win_rate": 1.0, "trades": 0, "wins": 0} for s in symbols}

        # Risk manager
        if risk_manager is None:
            # create a default risk manager using client's defaults
            self.risk_manager = RiskManager(client, RiskManagerConfig())
        else:
            self.risk_manager = risk_manager

        # pause until midnight when daily loss exceeded
        self._paused_until_midnight = False
        self._resume_task: Optional[asyncio.Task] = None

    # ---------------------- Управление ----------------------
    def stop(self):
        self._stop_event.set()

    async def stop_async(self):
        self.stop()

    # ---------------------- Основной цикл ----------------------
    async def run(self):
        logger.info(f"Strategy started for symbols: {self.symbols}")
        state.push_log(f"Strategy started for symbols: {self.symbols}")

        while not self._stop_event.is_set():
            try:
                signal_msg = await self.client.get_signal()

                # sentinel to break loop
                if isinstance(signal_msg, dict) and signal_msg.get("_meta") == "shutdown":
                    logger.info("Received shutdown sentinel in strategy loop")
                    self._stop_event.set()
                    break

                await self._process_signal(signal_msg)
                await self._monitor_positions()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in strategy loop: {e}")
                state.push_log(f"Error in strategy loop: {e}")

        logger.info("Strategy run loop finished")

    # ---------------------- Обработка сигналов ----------------------
    async def _process_signal(self, signal: Dict[str, Any]):
        if not signal or not isinstance(signal, dict):
            return

        topic = signal.get("topic", "")
        trade_obj = signal.get("trade") or signal.get("data") or signal.get("trade_data")
        if trade_obj is None:
            return

        trades = trade_obj if isinstance(trade_obj, list) else [trade_obj]

        for t in trades:
            if not isinstance(t, dict):
                continue

            symbol = t.get("symbol") or next((s for s in self.symbols if s in topic), None)
            if not symbol:
                continue

            try:
                price = float(t.get("price", 0) or 0)
                vol = float(t.get("vol", 0) or 0)
                side = t.get("side", "Buy")
            except (ValueError, TypeError):
                continue

            if price <= 0:
                continue

            # store histories
            ts_sec = t.get("ts", datetime.utcnow().timestamp())
            try:
                ts_sec_f = float(ts_sec)
            except Exception:
                ts_sec_f = datetime.utcnow().timestamp()

            self.trade_history[symbol].append({"ts": ts_sec_f, "price": price, "vol": vol, "side": side})
            self.price_history[symbol].append(price)
            self.vwap_history[symbol].append((price, vol))

            # aggregator expects ms timestamps inside 'trade' dict (we wrap)
            t_copy = {"symbol": symbol, "ts": int(ts_sec_f * 1000), "price": price, "vol": vol, "side": side}
            self.aggregators[symbol].add_trade({"trade": t_copy})

            features = self.aggregators[symbol].feature_vector(self.lookback_ms)
            self.strategy_performance[symbol]["features"] = features

            for strategy_name, enabled in self.enabled_strategies.items():
                if not enabled:
                    continue
                method = getattr(self, f"_{strategy_name}", None)
                if method:
                    try:
                        await method(symbol, features=features)
                    except TypeError:
                        await method(symbol)
                    except Exception as e:
                        logger.exception(f"Strategy {strategy_name} failed for {symbol}: {e}")

    # ---------------------- Вспомогательные ----------------------
    def _now(self):
        return datetime.utcnow()

    def _pause_until_midnight(self):
        if self._paused_until_midnight:
            return
        self._paused_until_midnight = True
        now = datetime.utcnow()
        next_midnight = (now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1))
        delay = (next_midnight - now).total_seconds()
        logger.warning(f"Pausing trading until next UTC midnight (resume in {int(delay)}s) due to daily loss limit.")
        state.push_log("Trading paused until next UTC midnight due to daily loss limit.")
        if self._resume_task is None or self._resume_task.done():
            self._resume_task = asyncio.create_task(self._resume_after(delay))

    async def _resume_after(self, delay: float):
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            return
        # reset daily counters inside risk manager
        self.risk_manager._reset_daily_if_needed()
        self._paused_until_midnight = False
        logger.info("Resuming trading after day reset.")
        state.push_log("Resuming trading after day reset.")

    def _is_paused(self) -> bool:
        return self._paused_until_midnight

    # ---------------------- Стратегии ----------------------
    async def _volume_spike(self, symbol: str, features: Optional[Dict[str, float]] = None):
        agg = self.aggregators[symbol]
        recent = agg.get_recent(self.lookback_ms)
        if len(recent) < 10:
            return
        last5 = recent[-5:]
        prev = recent[:-5]
        recent_vol = sum(t["vol"] for t in last5)
        prev_vol = sum(t["vol"] for t in prev) if prev else 0
        if prev_vol <= 0:
            return
        if recent_vol / prev_vol > 2.0:
            last_price = self.price_history[symbol][-1] if self.price_history[symbol] else None
            await self._safe_place_order(symbol, "Buy", price=last_price)

    async def _ema_crossover(self, symbol: str, features: Optional[Dict[str, float]] = None,
                             short_period: int = 5, long_period: int = 20):
        prices = list(self.price_history[symbol])
        if len(prices) < long_period:
            return
        short_ema = self._ema(prices, short_period)
        long_ema = self._ema(prices, long_period)
        last_signal = self._ema_last_signal[symbol]

        if short_ema > long_ema and last_signal != "Buy":
            last_price = prices[-1]
            await self._safe_place_order(symbol, "Buy", price=last_price)
            self._ema_last_signal[symbol] = "Buy"
        elif short_ema < long_ema and last_signal != "Sell":
            last_price = prices[-1]
            await self._safe_place_order(symbol, "Sell", price=last_price)
            self._ema_last_signal[symbol] = "Sell"

    def _ema(self, prices: List[float], period: int):
        if len(prices) < period:
            return 0.0
        ema = sum(prices[:period]) / period
        multiplier = 2 / (period + 1)
        for price in prices[period:]:
            ema = (price - ema) * multiplier + ema
        return ema

    async def _price_momentum(self, symbol: str, features: Optional[Dict[str, float]] = None, threshold: float = 0.005):
        mom = (features or {}).get("momentum", 0.0)
        last_price = self.price_history[symbol][-1] if self.price_history[symbol] else None
        if mom > threshold:
            await self._safe_place_order(symbol, "Buy", price=last_price)
        elif mom < -threshold:
            await self._safe_place_order(symbol, "Sell", price=last_price)

    async def _vwap(self, symbol: str, features: Optional[Dict[str, float]] = None):
        vwap_val = (features or {}).get("vwap")
        if not vwap_val:
            return
        current_price = self.price_history[symbol][-1] if self.price_history[symbol] else None
        if current_price is None:
            return
        if current_price > vwap_val * 1.001:
            await self._safe_place_order(symbol, "Buy", price=current_price)
        elif current_price < vwap_val * 0.999:
            await self._safe_place_order(symbol, "Sell", price=current_price)

    async def _order_book_imbalance(self, symbol: str, features: Optional[Dict[str, float]] = None):
        imb = (features or {}).get("imbalance", 0.0)
        current_price = self.price_history[symbol][-1] if self.price_history[symbol] else None
        if current_price is None:
            return
        if imb > 0.6:
            await self._safe_place_order(symbol, "Buy", price=current_price)
        elif imb < -0.6:
            await self._safe_place_order(symbol, "Sell", price=current_price)

    async def _candlestick_pattern(self, symbol: str, features: Optional[Dict[str, float]] = None):
        prices = list(self.price_history[symbol])
        if len(prices) < 3:
            return
        if prices[-3] < prices[-2] < prices[-1]:
            await self._safe_place_order(symbol, "Buy", price=prices[-1])
        elif prices[-3] > prices[-2] > prices[-1]:
            await self._safe_place_order(symbol, "Sell", price=prices[-1])

    # ---------------------- Ордеры / интеграция с RiskManager ----------------------
    async def _safe_place_order(self, symbol: str, side: str, qty: Optional[float] = None, price: Optional[float] = None):
        # Do not place new trades if paused due to daily loss
        if self._is_paused():
            logger.debug("Strategy paused due to daily loss limit; skipping order attempt.")
            return {"status": "paused"}

        now = datetime.utcnow()
        last_trade = self._last_trade_time.get(symbol)
        if last_trade and (now - last_trade) < self.trade_cooldown:
            return {"status": "cooldown", "price": price or (self.price_history[symbol][-1] if self.price_history[symbol] else None)}

        # Ensure we have a current price
        current_price = price or (self.price_history[symbol][-1] if self.price_history[symbol] else None)
        if current_price is None:
            logger.debug(f"No current price for {symbol} - skipping order")
            return {"status": "no_price"}

        # Estimate volatility to compute a candidate stop price
        recent_prices = list(self.price_history[symbol])[-20:]
        volatility = (max(recent_prices) - min(recent_prices)) if recent_prices else 0.0
        if side == "Buy":
            stop_price = current_price - volatility * 0.5
        else:
            stop_price = current_price + volatility * 0.5

        # If user provided qty, respect it; otherwise calculate via RiskManager
        if qty is None:
            # call RiskManager to compute qty based on risk
            res = await self.risk_manager.calculate_qty_from_risk(symbol, entry_price=current_price, stop_price=stop_price)
            if not res.get("ok"):
                warnings = res.get("warnings", [])
                logger.info(f"RiskManager declined order for {symbol}: {warnings}")
                # if daily limit reached, pause until midnight
                if "daily_loss_limit_reached" in warnings:
                    self._pause_until_midnight()
                return {"status": "risk_rejected", "warnings": warnings}
            qty = res.get("qty")
            if qty is None or qty <= 0:
                return {"status": "qty_zero"}

        # ensure qty respects min quantity
        min_q = self.risk_manager._symbol_min_qty(symbol)
        if qty < min_q:
            logger.info(f"Calculated qty {qty} below min {min_q} for {symbol} - not placing order")
            return {"status": "qty_below_min", "qty": qty, "min_qty": min_q}

        # place order
        self._last_trade_time[symbol] = now
        result = await self._place_order(symbol, side, qty, current_price)
        return result

    async def _place_order(self, symbol: str, side: str, qty: float, price: Optional[float] = None):
        try:
            current_price = price or (self.price_history[symbol][-1] if self.price_history[symbol] else 0)

            recent_prices = list(self.price_history[symbol])[-20:]
            volatility = (max(recent_prices) - min(recent_prices)) if recent_prices else 0

            if side == "Buy":
                sl_price = current_price - volatility * 0.5
                tp_price = current_price + volatility
            else:
                sl_price = current_price + volatility * 0.5
                tp_price = current_price - volatility

            order = await self.client.place_order(symbol, side, qty, price)
            status = order.get("status", "unknown")
            executed_price = order.get("price", current_price)

            state.push_log(f"[Strategy] {side} {symbol} {qty} -> {status}")
            logger.info(f"[Strategy] {side} {symbol} {qty} -> {status}")

            if status == "filled":
                # register open position with risk manager
                self.open_positions[symbol] = {
                    "side": side,
                    "qty": qty,
                    "price": executed_price,
                    "sl": sl_price,
                    "tp": tp_price,
                }
                try:
                    self.risk_manager.register_open_position(symbol, qty, executed_price)
                except Exception as e:
                    logger.debug(f"Failed to register open position in RiskManager: {e}")

            return {"status": status, "price": executed_price}
        except Exception as e:
            logger.error(f"Failed to place order {side} {symbol}: {e}")
            state.push_log(f"Failed to place order {side} {symbol}: {e}")
            return {"status": "error", "price": price or 0}

    # ---------------------- Мониторинг позиций ----------------------
    async def _monitor_positions(self):
        for symbol, position in list(self.open_positions.items()):
            if position is None or not self.price_history[symbol]:
                continue

            current_price = self.price_history[symbol][-1]
            side, qty, sl, tp = position["side"], position["qty"], position["sl"], position["tp"]

            try:
                # Stop Loss
                if (side == "Buy" and current_price <= sl) or (side == "Sell" and current_price >= sl):
                    await self._close_position(symbol, qty, reason="StopLoss")
                    self.open_positions[symbol] = None
                    continue

                # Take Profit: partial close
                if (side == "Buy" and current_price >= tp) or (side == "Sell" and current_price <= tp):
                    partial_qty = qty / 2
                    await self._close_position(symbol, partial_qty, reason="TakeProfit")
                    remaining_qty = qty - partial_qty
                    if remaining_qty <= 0.0000001:
                        self.open_positions[symbol] = None
                    else:
                        self.open_positions[symbol]["qty"] = remaining_qty
                        # Recalculate SL/TP for remaining
                        if side == "Buy":
                            self.open_positions[symbol]["sl"] = current_price - (tp - current_price) * 0.5
                            self.open_positions[symbol]["tp"] = current_price + (tp - current_price)
                        else:
                            self.open_positions[symbol]["sl"] = current_price + (current_price - tp) * 0.5
                            self.open_positions[symbol]["tp"] = current_price - (current_price - tp)

            except Exception as e:
                logger.error(f"Error monitoring position for {symbol}: {e}")
                state.push_log(f"Error monitoring position for {symbol}: {e}")

    async def _close_position(self, symbol: str, qty: float, reason: str = "Manual"):
        try:
            position = self.open_positions.get(symbol)
            if not position:
                return
            side = "Sell" if position["side"] == "Buy" else "Buy"
            # place a market order to close
            order = await self.client.place_order(symbol, side, qty)
            executed_price = order.get("price") if isinstance(order, dict) else None
            if executed_price is None:
                # fallback to current price
                executed_price = self.price_history[symbol][-1] if self.price_history[symbol] else 0

            # compute realized PnL in USD (quote) for the closed portion
            entry_price = position.get("price", 0)
            if position["side"] == "Buy":
                pnl = (executed_price - entry_price) * qty
            else:
                pnl = (entry_price - executed_price) * qty

            # register in risk manager
            try:
                self.risk_manager.register_close_position(symbol, qty, executed_price)
                self.risk_manager.register_trade_result(pnl)
            except Exception as e:
                logger.debug(f"Failed to register close/trade result in RiskManager: {e}")

            state.push_log(f"Closed {qty} of {symbol} due to {reason} (pnl={pnl:.6f})")
            logger.info(f"Closed {qty} of {symbol} due to {reason} (pnl={pnl:.6f})")
        except Exception as e:
            logger.error(f"Failed to close position {symbol}: {e}")
            state.push_log(f"Failed to close position {symbol}: {e}")

    # ---------------------- Управление балансом ----------------------
    def set_balance(self, symbol: str, balance: float):
        if symbol not in self.symbols:
            logger.warning(f"Attempted to set balance for unknown symbol: {symbol}")
            return
        self.strategy_performance[symbol] = self.strategy_performance.get(symbol, {})
        self.strategy_performance[symbol]["balance"] = balance
        logger.info(f"Balance for {symbol} set to {balance}")
        state.push_log(f"Balance for {symbol} set to {balance}")
