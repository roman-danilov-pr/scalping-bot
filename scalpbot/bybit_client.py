# scalpbot/bybit_client.py
"""
Improved BybitClient.
Changes made compared to original:
- Normalizes incoming WS tick formats into a single canonical trade dict: {
    'symbol': str, 'ts': float (sec), 'price': float, 'vol': float, 'side': 'Buy'/'Sell'
  }
  and always pushes: {"topic": topic, "trade": normalized_trade}
- signals_queue is bounded to avoid unbounded memory growth. If full, oldest item is dropped.
- Paper-mode order simulation: returns realistic 'filled' response with price, order_id, filled_qty.
  Keeps last known price per-symbol (from WS) for market fills and applies a simple slippage model.
- Better logging and safe parsing of WS payloads.
- Small helper methods added: _normalize_trade, _safe_json_load.

Note: graceful shutdown coordination (strategy stop / main stop handler) still needs small changes
in `main.py` / `strategy.py` to guarantee immediate wakeup of the strategy loop. We'll fix those
in subsequent steps as requested.
"""

import asyncio
import logging
import json
import uuid
import random
import httpx
import websockets
from datetime import datetime
from typing import Any, Dict, Optional

logger = logging.getLogger("scalpbot.bybit")


class BybitClient:
    """Client for Bybit public WS + basic REST wrapper and paper-mode simulation.

    Canonical trade format pushed to signals_queue:
        {
            "topic": "publicTrade.BTCUSDT",
            "trade": {
                "symbol": "BTCUSDT",
                "ts": 1690000000.123,    # seconds since epoch (float)
                "price": 27342.5,        # float
                "vol": 0.001,           # float (base asset qty)
                "side": "Buy"         # "Buy" or "Sell"
            }
        }
    """

    DEFAULT_QUEUE_MAXSIZE = 20_000

    def __init__(self, symbols, paper: bool = True, initial_balance: float = 10_000.0):
        self.symbols = symbols
        self.paper = paper
        self.ws_url = "wss://stream.bybit.com/v5/public/linear"
        self.rest_base = "https://api.bybit.com"
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.connected = False
        self._listen_task: Optional[asyncio.Task] = None
        self.signals_queue: asyncio.Queue = asyncio.Queue(maxsize=self.DEFAULT_QUEUE_MAXSIZE)
        self.client = httpx.AsyncClient(timeout=10)
        self._reconnect_delay = 5
        self._stop = False

        # paper simulation state
        self._paper_balance = {"USDT": float(initial_balance)}
        self._paper_positions: Dict[str, Dict[str, Any]] = {}  # symbol -> position dict
        self._paper_orders: Dict[str, Dict[str, Any]] = {}  # order_id -> order info

        # store last seen prices for market simulations
        self._last_price: Dict[str, float] = {s: 0.0 for s in symbols}

        # simple metrics
        self._dropped_ticks = 0

    # ------------------ Connection handling ------------------
    async def connect(self):
        """Подключение к Bybit WS и запуск слушателя."""
        while not self._stop:
            try:
                logger.info("Connecting to Bybit WS...")
                self.ws = await websockets.connect(self.ws_url, ping_interval=10, ping_timeout=5)
                self.connected = True
                await self.subscribe_public_trades(self.symbols)
                self._listen_task = asyncio.create_task(self._listen())
                logger.info("Bybit WS connected")
                break
            except Exception as e:
                logger.warning(f"WS connection failed: {e}, retry in {self._reconnect_delay}s")
                await asyncio.sleep(self._reconnect_delay)

    async def disconnect(self):
        """Отключение WS и закрытие HTTP клиента. Также помечаем stop и пытаемся разбудить слушателей."""
        self._stop = True
        if self.connected:
            logger.info("Disconnecting Bybit WS...")
            if self._listen_task:
                self._listen_task.cancel()
            try:
                await self.ws.close()
            except Exception:
                pass
            self.connected = False
        # attempt to wake up consumers by placing a sentinel (non-trade) if queue not full
        try:
            self._put_nowait_safe({"_meta": "shutdown"})
        except Exception:
            pass
        await self.client.aclose()

    async def subscribe_public_trades(self, symbols):
        """Подписка на publicTrade.<symbol> топики"""
        for s in symbols:
            topic = f"publicTrade.{s}"
            sub_msg = {"op": "subscribe", "args": [topic]}
            try:
                await self.ws.send(json.dumps(sub_msg))
                logger.info(f"Subscribed to {topic}")
            except Exception as e:
                logger.warning(f"Failed to subscribe {topic}: {e}")

    async def subscribe_kline(self, symbol, interval="1"):
        topic = f"klineV2.{interval}.{symbol}"
        sub_msg = {"op": "subscribe", "args": [topic]}
        await self.ws.send(json.dumps(sub_msg))
        logger.info(f"Subscribed to {topic}")

    async def _listen(self):
        """Асинхронный слушатель WS сообщений. Нормализует и пушит canonical-трейды в очередь."""
        try:
            async for raw_msg in self.ws:
                # raw_msg expected to be JSON text
                await self._handle_message(raw_msg)
        except asyncio.CancelledError:
            logger.info("WS listen task cancelled")
        except Exception as e:
            logger.error(f"WS listen error: {e}")
            self.connected = False
            if not self._stop:
                await self._reconnect()

    async def _reconnect(self):
        if self._stop:
            return
        logger.info(f"Reconnecting in {self._reconnect_delay}s...")
        await asyncio.sleep(self._reconnect_delay)
        await self.connect()

    async def _handle_message(self, raw_msg: str):
        data = self._safe_json_load(raw_msg)
        if data is None:
            return

        # Bybit public stream wrap: often contains 'topic' and 'data'
        topic = data.get("topic") or data.get("_topic") or None
        payload = data.get("data") or data.get("trade") or data.get("result") or None

        # If payload is a list of trades
        if topic and payload:
            # normalize various payload shapes
            items = payload if isinstance(payload, list) else [payload]
            for item in items:
                norm = self._normalize_trade(item)
                if not norm:
                    continue
                # update last seen price
                sym = norm.get("symbol")
                if sym and norm.get("price"):
                    self._last_price[sym] = norm["price"]
                # push canonical message
                msg = {"topic": topic, "trade": norm}
                # try to put without blocking; if full, drop oldest
                self._put_nowait_safe(msg)
        else:
            # handle subscription responses or errors
            if data.get("success") is False:
                logger.warning(f"Subscription/WS returned failure: {data}")
            # other messages are ignored for now

    # ------------------ Helpers: normalization / queue mgmt ------------------
    def _safe_json_load(self, raw: str) -> Optional[Dict[str, Any]]:
        try:
            return json.loads(raw)
        except Exception:
            return None

    def _normalize_trade(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Try to extract a canonical trade from a raw item from Bybit WS.

        Supports several possible key names encountered in different public feeds.
        Returns None if not enough information.
        """
        try:
            # symbol
            symbol = item.get("symbol") or item.get("s") or item.get("S") or item.get("instrument_name")

            # timestamp: Bybit sometimes gives 'T' in ms or 't'
            ts_ms = None
            for k in ("T", "t", "timestamp_ms", "ts"):
                if k in item:
                    try:
                        ts_ms = int(item[k])
                        break
                    except Exception:
                        pass
            if ts_ms:
                ts = ts_ms / 1000.0
            else:
                # if no ts provided, use server local time
                ts = datetime.utcnow().timestamp()

            # price
            price = None
            for k in ("p", "price", "px", "price_str"):
                if k in item:
                    try:
                        price = float(item[k])
                        break
                    except Exception:
                        pass

            # volume / size
            vol = None
            for k in ("v", "size", "q", "qty", "volume"):
                if k in item:
                    try:
                        vol = float(item[k])
                        break
                    except Exception:
                        pass

            # side
            side = None
            for k in ("S", "side", "direction"):
                if k in item:
                    side_raw = str(item[k])
                    if side_raw.lower().startswith("b"):
                        side = "Buy"
                    elif side_raw.lower().startswith("s"):
                        side = "Sell"
                    else:
                        side = side_raw
                    break

            # some feeds provide nested fields
            # fallback to try parsing 'data' shapes
            if symbol is None and isinstance(item.get("symbol"), dict):
                symbol = item.get("symbol").get("symbol")

            if price is None or vol is None or side is None:
                # Sometimes the trade payload uses other names — try common nested cases
                # but don't overcomplicate: if essential fields missing, skip
                # (we still return partial if symbol & price exist and vol default to 0)
                pass

            if symbol is None or price is None:
                return None

            if vol is None:
                vol = 0.0
            if side is None:
                side = "Buy"

            return {"symbol": symbol, "ts": ts, "price": price, "vol": vol, "side": side}
        except Exception:
            return None

    def _put_nowait_safe(self, item: Dict[str, Any]):
        """Try to put an item to the signals_queue.

        If the queue is full, drop the oldest item to make room (to keep most recent data).
        This keeps memory bounded under bursts.
        """
        q = self.signals_queue
        try:
            q.put_nowait(item)
        except asyncio.QueueFull:
            try:
                # drop oldest
                _ = q.get_nowait()
                q.put_nowait(item)
                self._dropped_ticks += 1
                logger.debug("Dropped oldest tick to make room in signals_queue")
            except Exception:
                # if still failing, increment counter and swallow
                self._dropped_ticks += 1

    # ------------------ REST utilities ------------------
    async def get_server_time(self):
        try:
            r = await self.client.get(f"{self.rest_base}/v2/public/time")
            if r.status_code == 200:
                ts = r.json().get("time_now")
                if ts:
                    return datetime.fromtimestamp(float(ts))
        except Exception as e:
            logger.warning(f"REST GET time failed: {e}")
        return datetime.utcnow()

    # ------------------ Paper / Live order placement ------------------
    async def place_order(self, symbol: str, side: str, qty: float, price: Optional[float] = None, order_type: str = "Market") -> Dict[str, Any]:
        """Place an order. In paper mode we simulate execution and return a filled order.

        Returned dict (paper): {"status": "filled", "price": executed_price, "order_id": str, "filled_qty": qty}
        For live mode currently returns payload (not implemented) so upper layers can integrate real REST signing.
        """
        if self.paper:
            # simulate fill
            market_price = price or self._last_price.get(symbol) or 0.0
            if market_price <= 0:
                # fallback: if we don't have a price yet, use a small random around 0 (not ideal)
                market_price = float(random.uniform(1.0, 100.0))

            # simple slippage model: market order moves price by tiny fraction proportional to qty
            slippage = max(0.0, 0.0005 * (qty / 0.01))  # scale slippage with qty, baseline 0.05%
            executed_price = market_price * (1 + slippage if side.lower().startswith("b") else 1 - slippage)

            order_id = str(uuid.uuid4())
            order = {
                "order_id": order_id,
                "symbol": symbol,
                "side": side,
                "qty": qty,
                "price": executed_price,
                "status": "filled",
                "filled_qty": qty,
                "ts": datetime.utcnow().timestamp(),
            }
            # update paper state
            self._paper_orders[order_id] = order

            # balance/position naive update: assume USDT quote, symbol like BTCUSDT
            base_asset = symbol.replace("USDT", "") if symbol.endswith("USDT") else symbol
            cost = executed_price * qty
            # commission simple model: 0.02% of cost
            commission = cost * 0.0002
            if side.lower().startswith("b"):
                # decrease USDT balance
                self._paper_balance["USDT"] = self._paper_balance.get("USDT", 0.0) - cost - commission
                # create/update position
                pos = self._paper_positions.get(symbol, {"qty": 0.0, "avg_price": 0.0})
                total_qty = pos["qty"] + qty
                if total_qty > 0:
                    pos["avg_price"] = (pos["avg_price"] * pos["qty"] + executed_price * qty) / total_qty
                else:
                    pos["avg_price"] = 0.0
                pos["qty"] = total_qty
                self._paper_positions[symbol] = pos
            else:
                # Sell: increase USDT balance, reduce base asset holdings
                self._paper_balance["USDT"] = self._paper_balance.get("USDT", 0.0) + cost - commission
                pos = self._paper_positions.get(symbol, {"qty": 0.0, "avg_price": 0.0})
                pos["qty"] = max(0.0, pos.get("qty", 0.0) - qty)
                # avg_price left as-is (could be recomputed)
                self._paper_positions[symbol] = pos

            logger.info(f"[PAPER] Filled {side} {symbol} {qty} @ {executed_price:.8f} (order_id={order_id})")
            return {"status": "filled", "price": executed_price, "order_id": order_id, "filled_qty": qty}

        # Live mode: prepare payload and return it (integration with signed REST not implemented here)
        payload = {
            "symbol": symbol,
            "side": side,
            "order_type": order_type,
            "qty": qty,
            "time_in_force": "GoodTillCancel",
        }
        if price:
            payload["price"] = price
        logger.info(f"[LIVE] Order payload prepared: {payload}")
        # TODO: implement signed REST call to create order and return result
        return {"status": "prepared", "payload": payload}

    async def get_balance(self) -> Dict[str, float]:
        """Return paper balance or live balance (not implemented)."""
        if self.paper:
            # return shallow copy
            return dict(self._paper_balance)
        # TODO: call REST to return real balance
        return {"USDT": 0.0}

    # ------------------ Signal API ------------------
    async def get_signal(self):
        """Return the next canonical signal from the internal queue.

        Note: consumers expect a dict with keys 'topic' and 'trade'.
        """
        item = await self.signals_queue.get()
        return item

    # ------------------ Utilities for inspection (paper-mode state) ------------------
    def paper_state(self) -> Dict[str, Any]:
        return {
            "balance": dict(self._paper_balance),
            "positions": dict(self._paper_positions),
            "orders": dict(self._paper_orders),
            "last_price": dict(self._last_price),
            "dropped_ticks": self._dropped_ticks,
        }


# End of file
