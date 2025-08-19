# scalpbot/strategy.py
"""
Strategy prepared for testing:
- Uses injected feed (feed.get_signal()) and injected executor (executor.execute_order()).
- Emits structured events via event_sink for every important step.
- Stores open_positions with open_ts and supports max_trade_time-based forced close.
- When executor returns 'commission' it is deducted from realized PnL before registering trade result.
"""

import asyncio
import logging
import inspect
import json
from collections import deque
from datetime import datetime, timedelta
from typing import Deque, Dict, Any, Optional, List, Callable

from signals import TradeAggregator
from shared_state import get_state
from risk_manager import RiskManager, RiskManagerConfig

logger = logging.getLogger("scalpbot.strategy")
state = get_state()


class Strategy:
    def __init__(self,
                 feed,
                 symbols: List[str],
                 window: int = 60,
                 enabled_strategies: Optional[Dict[str, bool]] = None,
                 risk_manager: Optional[RiskManager] = None,
                 executor: Optional[Any] = None,
                 event_sink: Optional[Callable[[Dict[str, Any]], Any]] = None,
                 max_trade_time: timedelta = timedelta(minutes=60),
                 ):
        self.feed = feed
        self.executor = executor
        self.event_sink = event_sink

        self.symbols = symbols
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

        self._stop_event = asyncio.Event()

        self.trade_history: Dict[str, Deque[Dict[str, Any]]] = {s: deque(maxlen=1000) for s in symbols}
        self.price_history: Dict[str, Deque[float]] = {s: deque(maxlen=1000) for s in symbols}
        self.vwap_history: Dict[str, Deque[Any]] = {s: deque(maxlen=1000) for s in symbols}

        self.aggregators: Dict[str, TradeAggregator] = {s: TradeAggregator(window=window) for s in symbols}

        self._ema_last_signal: Dict[str, Optional[str]] = {s: None for s in symbols}
        self._last_trade_time: Dict[str, datetime] = {s: datetime.min for s in symbols}
        self.trade_cooldown = timedelta(seconds=5)

        # open_positions per symbol: dict with fields side, qty, price, sl, tp, open_ts
        self.open_positions: Dict[str, Optional[Dict[str, Any]]] = {s: None for s in symbols}

        self.strategy_performance: Dict[str, Dict[str, Any]] = {
            s: {"win_rate": 1.0, "trades": 0, "wins": 0} for s in symbols
        }

        self.risk_manager = risk_manager or RiskManager(feed, RiskManagerConfig())

        self._paused_until_midnight = False
        self._resume_task: Optional[asyncio.Task] = None

        # max time to hold a trade (if exceeded -> force close)
        self.max_trade_time = max_trade_time

        # internal decision log
        self._decision_log: Deque[Dict[str, Any]] = deque(maxlen=2000)

    # -------------- events --------------
    async def _emit(self, type_: str, **payload):
        event = {"ts": datetime.utcnow().timestamp(), "type": type_, **payload}
        self._decision_log.append(event)
        if self.event_sink:
            try:
                if inspect.iscoroutinefunction(self.event_sink):
                    await self.event_sink(event)
                else:
                    self.event_sink(event)
            except Exception as e:
                logger.debug(f"event_sink error: {e}")
        # concise push to shared log for UI
        try:
            msg = {"type": event.get("type"), "symbol": event.get("symbol")}
            state.push_log(f"EV {json.dumps(msg, ensure_ascii=False)}")
        except Exception:
            pass

    # -------------- control --------------
    def stop(self):
        self._stop_event.set()

    async def stop_async(self):
        self.stop()

    def update_enabled_strategies(self, flags: Dict[str, bool]):
        self.enabled_strategies.update(flags)
        asyncio.create_task(self._emit("strategies_updated", flags=self.enabled_strategies.copy()))

    # -------------- main loop --------------
    async def run(self):
        logger.info(f"Strategy started for symbols: {self.symbols}")
        state.push_log(f"Strategy started for symbols: {self.symbols}")
        await self._emit("started", symbols=self.symbols, window=self.window)

        while not self._stop_event.is_set():
            try:
                signal_msg = await self.feed.get_signal()

                if isinstance(signal_msg, dict) and signal_msg.get("_meta") == "shutdown":
                    logger.info("Shutdown sentinel received")
                    await self._emit("shutdown_signal")
                    self._stop_event.set()
                    break

                await self._process_signal(signal_msg)
                await self._monitor_positions()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in strategy loop: {e}")
                state.push_log(f"Error in strategy loop: {e}")
                await self._emit("error", message=str(e))

        logger.info("Strategy finished")
        await self._emit("stopped")

    # -------------- signal processing --------------
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

            ts_sec = t.get("ts", datetime.utcnow().timestamp())
            try:
                ts_sec_f = float(ts_sec)
            except Exception:
                ts_sec_f = datetime.utcnow().timestamp()

            self.trade_history[symbol].append({"ts": ts_sec_f, "price": price, "vol": vol, "side": side})
            self.price_history[symbol].append(price)
            self.vwap_history[symbol].append((price, vol))

            t_copy = {"symbol": symbol, "ts": int(ts_sec_f * 1000), "price": price, "vol": vol, "side": side}
            self.aggregators[symbol].add_trade({"trade": t_copy})

            await self._emit("tick", symbol=symbol, price=price, vol=vol)

            features = self.aggregators[symbol].feature_vector(self.lookback_ms)
            self.strategy_performance[symbol]["features"] = features
            await self._emit("features", symbol=symbol, features=features)

            # run enabled strategies
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
                        await self._emit("error", symbol=symbol, strategy=strategy_name, message=str(e))

    # -------------- helpers --------------
    def _now(self):
        return datetime.utcnow()

    def _pause_until_midnight(self):
        if self._paused_until_midnight:
            return
        self._paused_until_midnight = True
        now = datetime.utcnow()
        next_midnight = (now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1))
        delay = (next_midnight - now).total_seconds()
        logger.warning("Pausing trading until next UTC midnight due to daily loss limit.")
        state.push_log("Trading paused until next UTC midnight due to daily loss limit.")
        asyncio.create_task(self._emit("pause_due_to_daily_loss", resume_in_s=int(delay)))
        if self._resume_task is None or self._resume_task.done():
            self._resume_task = asyncio.create_task(self._resume_after(delay))

    async def _resume_after(self, delay: float):
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            return
        self.risk_manager._reset_daily_if_needed()
        self._paused_until_midnight = False
        logger.info("Resuming trading after day reset.")
        state.push_log("Resuming trading after day reset.")
        await self._emit("resumed_after_day_reset")

    def _is_paused(self) -> bool:
        return self._paused_until_midnight

    # ---------------- strategies ----------------
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
            await self._emit("decision", symbol=symbol, strategy="volume_spike", action="Buy")
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
            await self._emit("decision", symbol=symbol, strategy="ema_crossover", action="Buy",
                             short_ema=short_ema, long_ema=long_ema)
            await self._safe_place_order(symbol, "Buy", price=last_price)
            self._ema_last_signal[symbol] = "Buy"
        elif short_ema < long_ema and last_signal != "Sell":
            last_price = prices[-1]
            await self._emit("decision", symbol=symbol, strategy="ema_crossover", action="Sell",
                             short_ema=short_ema, long_ema=long_ema)
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
            await self._emit("decision", symbol=symbol, strategy="price_momentum", action="Buy", momentum=mom)
            await self._safe_place_order(symbol, "Buy", price=last_price)
        elif mom < -threshold:
            await self._emit("decision", symbol=symbol, strategy="price_momentum", action="Sell", momentum=mom)
            await self._safe_place_order(symbol, "Sell", price=last_price)

    async def _vwap(self, symbol: str, features: Optional[Dict[str, float]] = None):
        vwap_val = (features or {}).get("vwap")
        if not vwap_val:
            return
        current_price = self.price_history[symbol][-1] if self.price_history[symbol] else None
        if current_price is None:
            return
        if current_price > vwap_val * 1.001:
            await self._emit("decision", symbol=symbol, strategy="vwap", action="Buy", vwap=vwap_val)
            await self._safe_place_order(symbol, "Buy", price=current_price)
        elif current_price < vwap_val * 0.999:
            await self._emit("decision", symbol=symbol, strategy="vwap", action="Sell", vwap=vwap_val)
            await self._safe_place_order(symbol, "Sell", price=current_price)

    async def _order_book_imbalance(self, symbol: str, features: Optional[Dict[str, float]] = None):
        imb = (features or {}).get("imbalance", 0.0)
        current_price = self.price_history[symbol][-1] if self.price_history[symbol] else None
        if current_price is None:
            return
        if imb > 0.6:
            await self._emit("decision", symbol=symbol, strategy="order_book_imbalance", action="Buy", imbalance=imb)
            await self._safe_place_order(symbol, "Buy", price=current_price)
        elif imb < -0.6:
            await self._emit("decision", symbol=symbol, strategy="order_book_imbalance", action="Sell", imbalance=imb)
            await self._safe_place_order(symbol, "Sell", price=current_price)

    async def _candlestick_pattern(self, symbol: str, features: Optional[Dict[str, float]] = None):
        prices = list(self.price_history[symbol])
        if len(prices) < 3:
            return
        if prices[-3] < prices[-2] < prices[-1]:
            await self._emit("decision", symbol=symbol, strategy="candlestick_pattern", action="Buy")
            await self._safe_place_order(symbol, "Buy", price=prices[-1])
        elif prices[-3] > prices[-2] > prices[-1]:
            await self._emit("decision", symbol=symbol, strategy="candlestick_pattern", action="Sell")
            await self._safe_place_order(symbol, "Sell", price=prices[-1])

    # -------------- order placement & execution --------------
    async def _safe_place_order(self, symbol: str, side: str, qty: Optional[float] = None, price: Optional[float] = None):
        if self._is_paused():
            await self._emit("paused", symbol=symbol, reason="daily_loss_limit")
            return {"status": "paused"}

        now = datetime.utcnow()
        last_trade = self._last_trade_time.get(symbol)
        if last_trade and (now - last_trade) < self.trade_cooldown:
            await self._emit("cooldown", symbol=symbol, cooldown_s=self.trade_cooldown.total_seconds())
            return {"status": "cooldown", "price": price or (self.price_history[symbol][-1] if self.price_history[symbol] else None)}

        current_price = price or (self.price_history[symbol][-1] if self.price_history[symbol] else None)
        if current_price is None:
            await self._emit("decision", symbol=symbol, action="skip_no_price")
            return {"status": "no_price"}

        recent_prices = list(self.price_history[symbol])[-20:]
        volatility = (max(recent_prices) - min(recent_prices)) if recent_prices else 0.0
        stop_price = current_price - volatility * 0.5 if side == "Buy" else current_price + volatility * 0.5

        risk_info: Dict[str, Any] = {}
        if qty is None:
            res = await self.risk_manager.calculate_qty_from_risk(symbol, entry_price=current_price, stop_price=stop_price)
            if not res.get("ok"):
                warnings = res.get("warnings", [])
                await self._emit("risk_rejected", symbol=symbol, warnings=warnings,
                                 current_price=current_price, stop_price=stop_price)
                if "daily_loss_limit_reached" in warnings:
                    self._pause_until_midnight()
                return {"status": "risk_rejected", "warnings": warnings}
            qty = float(res.get("qty") or 0.0)
            if qty <= 0:
                await self._emit("risk_rejected", symbol=symbol, warnings=["qty_zero"])
                return {"status": "qty_zero"}
            risk_info = {
                "notional": res.get("notional"),
                "required_margin": res.get("required_margin"),
                "est_fees": res.get("est_fees"),
                "est_slippage_cost": res.get("est_slippage_cost"),
                "warnings": res.get("warnings", []),
            }

        min_q = self.risk_manager._symbol_min_qty(symbol)
        if qty < min_q:
            await self._emit("risk_rejected", symbol=symbol,
                             warnings=["qty_below_minimum"], qty=qty, min_qty=min_q)
            return {"status": "qty_below_min", "qty": qty, "min_qty": min_q}

        self._last_trade_time[symbol] = now
        await self._emit("order_attempt", symbol=symbol, side=side, qty=qty,
                         price=current_price, stop_price=stop_price, **risk_info)
        result = await self._execute(symbol, side, qty, current_price)
        return result

    async def _execute(self, symbol: str, side: str, qty: float, price: Optional[float] = None) -> Dict[str, Any]:
        try:
            current_price = price or (self.price_history[symbol][-1] if self.price_history[symbol] else 0.0)
            recent_prices = list(self.price_history[symbol])[-20:]
            volatility = (max(recent_prices) - min(recent_prices)) if recent_prices else 0.0
            if side == "Buy":
                sl_price = current_price - volatility * 0.5
                tp_price = current_price + volatility
            else:
                sl_price = current_price + volatility * 0.5
                tp_price = current_price - volatility

            order: Dict[str, Any]
            if self.executor and hasattr(self.executor, "execute_order"):
                order = await self.executor.execute_order(symbol, side, qty, current_price,
                                                          order_type="Market",
                                                          context={"sl": sl_price, "tp": tp_price})
            elif hasattr(self.feed, "place_order"):
                # fallback to feed.place_order (paper client)
                order = await self.feed.place_order(symbol, side, qty, current_price)
            else:
                await self._emit("order_result", symbol=symbol, side=side, status="error_no_executor")
                return {"status": "error", "price": current_price}

            status = order.get("status", "unknown")
            executed_price = order.get("price", current_price)

            state.push_log(f"[Strategy] {side} {symbol} {qty} -> {status}")
            logger.info(f"[Strategy] {side} {symbol} {qty} -> {status}")
            await self._emit("order_result", symbol=symbol, side=side, qty=qty,
                             status=status, executed_price=executed_price, meta=order)

            if status == "filled":
                self.open_positions[symbol] = {
                    "side": side,
                    "qty": qty,
                    "price": executed_price,
                    "sl": sl_price,
                    "tp": tp_price,
                    "open_ts": datetime.utcnow(),
                }
                try:
                    self.risk_manager.register_open_position(symbol, qty, executed_price)
                except Exception as e:
                    logger.debug(f"Failed to register open position in RiskManager: {e}")
                await self._emit("position_opened", symbol=symbol, side=side, qty=qty,
                                 entry_price=executed_price, sl=sl_price, tp=tp_price,
                                 open_ts=datetime.utcnow().isoformat())
            return {"status": status, "price": executed_price, **({"commission": order.get("commission")} if order.get("commission") is not None else {})}
        except Exception as e:
            logger.error(f"Failed to execute order {side} {symbol}: {e}")
            state.push_log(f"Failed to execute order {side} {symbol}: {e}")
            await self._emit("error", symbol=symbol, message=str(e))
            return {"status": "error", "price": price or 0.0}

    # -------------- monitor & close --------------
    async def _monitor_positions(self):
        now = datetime.utcnow()
        for symbol, position in list(self.open_positions.items()):
            if position is None or not self.price_history[symbol]:
                continue
            current_price = self.price_history[symbol][-1]
            side, qty, sl, tp, open_ts = position["side"], position["qty"], position["sl"], position["tp"], position.get("open_ts", datetime.utcnow())
            try:
                # Stop Loss
                if (side == "Buy" and current_price <= sl) or (side == "Sell" and current_price >= sl):
                    await self._emit("sl_hit", symbol=symbol, side=side, sl=sl, price=current_price)
                    await self._close_position(symbol, qty, reason="StopLoss")
                    self.open_positions[symbol] = None
                    continue

                # Take Profit -> partial close
                if (side == "Buy" and current_price >= tp) or (side == "Sell" and current_price <= tp):
                    partial_qty = qty / 2
                    await self._emit("tp_partial", symbol=symbol, side=side, tp=tp,
                                     price=current_price, qty=partial_qty)
                    await self._close_position(symbol, partial_qty, reason="TakeProfit")
                    remaining_qty = qty - partial_qty
                    if remaining_qty <= 1e-8:
                        self.open_positions[symbol] = None
                    else:
                        self.open_positions[symbol]["qty"] = remaining_qty
                        if side == "Buy":
                            self.open_positions[symbol]["sl"] = current_price - (tp - current_price) * 0.5
                            self.open_positions[symbol]["tp"] = current_price + (tp - current_price)
                        else:
                            self.open_positions[symbol]["sl"] = current_price + (current_price - tp) * 0.5
                            self.open_positions[symbol]["tp"] = current_price - (current_price - tp)
                    continue

                # Max holding time check
                if self.max_trade_time and (now - open_ts) > self.max_trade_time:
                    await self._emit("max_time_exit", symbol=symbol, side=side, held_s=(now - open_ts).total_seconds())
                    await self._close_position(symbol, position["qty"], reason="MaxTradeTime")
                    self.open_positions[symbol] = None
                    continue

            except Exception as e:
                logger.error(f"Error monitoring position for {symbol}: {e}")
                state.push_log(f"Error monitoring position for {symbol}: {e}")
                await self._emit("error", symbol=symbol, message=str(e))

    async def _close_position(self, symbol: str, qty: float, reason: str = "Manual"):
        try:
            position = self.open_positions.get(symbol)
            if not position:
                return
            side = "Sell" if position["side"] == "Buy" else "Buy"

            order = await self._execute(symbol, side, qty,
                                        price=(self.price_history[symbol][-1] if self.price_history[symbol] else None))
            executed_price = float(order.get("price") or 0.0)
            commission = float(order.get("commission") or 0.0)

            entry_price = float(position.get("price", 0))
            if position["side"] == "Buy":
                pnl_gross = (executed_price - entry_price) * qty
            else:
                pnl_gross = (entry_price - executed_price) * qty

            # subtract commission (executor returns commission for the closing fill)
            pnl_net = pnl_gross - commission

            try:
                # register close exposure and net pnl
                self.risk_manager.register_close_position(symbol, qty, executed_price)
                self.risk_manager.register_trade_result(pnl_net)
            except Exception as e:
                logger.debug(f"Failed to register close/trade result in RiskManager: {e}")

            await self._emit("position_closed", symbol=symbol, qty=qty, reason=reason,
                             executed_price=executed_price, entry_price=entry_price,
                             pnl_gross=pnl_gross, commission=commission, pnl_net=pnl_net)
            state.push_log(f"Closed {qty} of {symbol} due to {reason} (pnl_gross={pnl_gross:.6f}, commission={commission:.6f}, pnl_net={pnl_net:.6f})")
            logger.info(f"Closed {qty} of {symbol} due to {reason} (pnl_net={pnl_net:.6f})")
        except Exception as e:
            logger.error(f"Failed to close position {symbol}: {e}")
            state.push_log(f"Failed to close position {symbol}: {e}")
            await self._emit("error", symbol=symbol, message=str(e))

    # -------------- misc --------------
    def set_balance(self, symbol: str, balance: float):
        if symbol not in self.symbols:
            logger.warning(f"Attempted to set balance for unknown symbol: {symbol}")
            return
        self.strategy_performance[symbol] = self.strategy_performance.get(symbol, {})
        self.strategy_performance[symbol]["balance"] = balance
        logger.info(f"Balance for {symbol} set to {balance}")
        state.push_log(f"Balance for {symbol} set to {balance}")

    def get_decision_log(self, n: int = 200) -> List[Dict[str, Any]]:
        return list(self._decision_log)[-n:]
