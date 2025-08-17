# scalpbot/risk_manager.py
"""
RiskManager for scalpbot.

Responsibilities:
- Calculate position quantity from defined risk per trade and stop-loss distance.
- Enforce limits: max risk per trade, max position value (% of balance), available margin for leveraged products.
- Round quantities to symbol-specific step sizes and respect minimum quantities.
- Track simple daily loss / drawdown limits (in-memory) and prevent new trades when breached.
- Load symbol filters (min_qty, step_size) from Bybit REST /v5/market/instruments-info.

Notes:
- This module expects a client that exposes:
    - client.client : httpx.AsyncClient
    - client.rest_base : base REST URL
    - client.get_balance() coroutine
"""
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Optional, Any, List

import logging

logger = logging.getLogger("scalpbot.risk")

@dataclass
class RiskManagerConfig:
    max_risk_per_trade: float = 0.01  # fraction of balance (1% default)
    max_position_value_pct: float = 0.1  # max position notional as percent of balance
    fee_rate: float = 0.0002  # maker/taker fee approximation (0.02%)
    slippage_rate: float = 0.0005  # expected slippage fraction
    min_qty_defaults: Dict[str, float] = field(default_factory=lambda: {})
    step_size_defaults: Dict[str, float] = field(default_factory=lambda: {})
    daily_loss_limit_usd: Optional[float] = None  # absolute USD limit (optional)
    daily_loss_limit_pct: Optional[float] = None  # fraction of balance (e.g., 0.02 => 2%)
    exposure_limit_usd: Optional[float] = None  # max total exposure across positions


class RiskManager:
    def __init__(self, client, cfg: Optional[RiskManagerConfig] = None):
        """
        client: BybitClient (has .client: httpx.AsyncClient and .rest_base)
        """
        self.client = client
        self.cfg = cfg or RiskManagerConfig()

        # simple in-memory trackers
        self._daily_pnl_usd = 0.0
        # The reset moment for daily counters (UTC midnight)
        self._daily_reset_time = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        self._open_positions_exposure = 0.0  # USD of open positions (notional)

    # ---------------------- Utilities ----------------------
    async def _get_balance_usd(self) -> float:
        try:
            bal = await self.client.get_balance()
            if isinstance(bal, dict):
                if "USDT" in bal:
                    return float(bal["USDT"])
                # fallback: first numeric value
                for v in bal.values():
                    try:
                        return float(v)
                    except Exception:
                        continue
        except Exception as e:
            logger.debug(f"Could not get balance from client: {e}")
        return 0.0

    def _symbol_min_qty(self, symbol: str) -> float:
        return float(self.cfg.min_qty_defaults.get(symbol, 0.000001))

    def _symbol_step_size(self, symbol: str) -> float:
        return float(self.cfg.step_size_defaults.get(symbol, 0.000001))

    def _round_down_qty(self, qty: float, step: float) -> float:
        if step <= 0:
            return qty
        n = math.floor(qty / step)
        return max(n * step, 0.0)

    def _reset_daily_if_needed(self):
        now = datetime.utcnow()
        if now - self._daily_reset_time >= timedelta(days=1):
            self._daily_reset_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
            self._daily_pnl_usd = 0.0

    def get_daily_pnl(self) -> float:
        self._reset_daily_if_needed()
        return self._daily_pnl_usd

    def get_daily_loss_limit_usd(self, balance: float) -> Optional[float]:
        if self.cfg.daily_loss_limit_usd is not None:
            return float(self.cfg.daily_loss_limit_usd)
        if self.cfg.daily_loss_limit_pct is not None:
            return float(balance) * float(self.cfg.daily_loss_limit_pct)
        return None

    # ---------------------- Public API ----------------------
    async def calculate_qty_from_risk(self, symbol: str, entry_price: float, stop_price: float,
                                      balance: Optional[float] = None, leverage: float = 1.0) -> Dict[str, Any]:
        """
        Calculate base-asset quantity to risk `max_risk_per_trade` of balance given stop distance.

        Returns dict with keys:
        - ok: bool
        - qty: float (rounded according to step size)
        - notional: qty * entry_price
        - required_margin: notional / leverage
        - est_fees, est_slippage_cost, warnings
        """
        self._reset_daily_if_needed()
        warnings: List[str] = []

        if entry_price <= 0 or stop_price <= 0:
            return {"ok": False, "warnings": ["invalid_price"]}

        if balance is None:
            balance = await self._get_balance_usd()

        # Enforce daily loss limit (if configured)
        daily_limit = self.get_daily_loss_limit_usd(balance)
        if daily_limit is not None and self._daily_pnl_usd <= -abs(daily_limit):
            return {"ok": False, "warnings": ["daily_loss_limit_reached"]}

        max_risk = self.cfg.max_risk_per_trade
        risk_amount = balance * max_risk

        stop_distance = abs(entry_price - stop_price)
        if stop_distance <= 0:
            return {"ok": False, "warnings": ["zero_stop_distance"]}

        raw_qty = risk_amount / stop_distance
        notional = raw_qty * entry_price

        # clamp by max position value
        max_pos_value = balance * self.cfg.max_position_value_pct
        if max_pos_value > 0 and notional > max_pos_value:
            raw_qty = max_pos_value / entry_price
            notional = raw_qty * entry_price
            warnings.append("clamped_to_max_position_value")

        # exposure limit
        if self.cfg.exposure_limit_usd is not None:
            if (self._open_positions_exposure + notional) > self.cfg.exposure_limit_usd:
                return {"ok": False, "warnings": ["exposure_limit_reached"]}

        required_margin = (notional / max(1.0, leverage))

        # estimate fees and slippage
        est_fees = notional * self.cfg.fee_rate
        est_slippage_cost = notional * self.cfg.slippage_rate
        total_need = required_margin + est_fees + est_slippage_cost

        if total_need > balance:
            affordable_notional = max(0.0, (balance - (est_fees + est_slippage_cost))) * max(1.0, leverage)
            if affordable_notional <= 0:
                return {"ok": False, "warnings": ["insufficient_balance_for_margin"]}
            raw_qty = affordable_notional / entry_price
            notional = raw_qty * entry_price
            required_margin = notional / max(1.0, leverage)
            warnings.append("reduced_for_margin")

        # round down to step size
        step = self._symbol_step_size(symbol)
        qty = self._round_down_qty(raw_qty, step)
        min_q = self._symbol_min_qty(symbol)
        if qty < min_q:
            return {"ok": False, "warnings": ["qty_below_minimum"], "qty": qty, "min_qty": min_q}

        # recompute after rounding
        notional = qty * entry_price
        required_margin = notional / max(1.0, leverage)
        est_fees = notional * self.cfg.fee_rate
        est_slippage_cost = notional * self.cfg.slippage_rate

        return {
            "ok": True,
            "qty": qty,
            "notional": notional,
            "required_margin": required_margin,
            "est_fees": est_fees,
            "est_slippage_cost": est_slippage_cost,
            "warnings": warnings,
        }

    async def can_open_position(self, symbol: str, qty: float, entry_price: float) -> bool:
        notional = qty * entry_price
        if self.cfg.exposure_limit_usd is None:
            return True
        return (self._open_positions_exposure + notional) <= self.cfg.exposure_limit_usd

    def register_open_position(self, symbol: str, qty: float, price: float):
        notional = qty * price
        self._open_positions_exposure += notional
        logger.debug(f"Registered open position {symbol} qty={qty} notional={notional:.6f}")

    def register_close_position(self, symbol: str, qty: float, price: float):
        notional = qty * price
        self._open_positions_exposure = max(0.0, self._open_positions_exposure - notional)
        logger.debug(f"Registered close position {symbol} qty={qty} notional={notional:.6f}")

    def register_trade_result(self, pnl_usd: float):
        """Record realized PnL (positive or negative) for the day."""
        self._reset_daily_if_needed()
        self._daily_pnl_usd += pnl_usd
        logger.info(f"Registered trade result pnl={pnl_usd:.6f} daily_pnl={self._daily_pnl_usd:.6f}")

    # ---------------------- Load symbol filters from exchange ----------------------
    async def load_symbol_filters_from_exchange(self, symbols: List[str]):
        """
        Query Bybit v5 instruments-info endpoint for each symbol and update step/min_qty.
        This operation is best-effort and will gracefully fallback when parsing fails.
        """
        http = getattr(self.client, "client", None)
        base = getattr(self.client, "rest_base", None)
        if http is None or base is None:
            logger.warning("Client does not provide http client or rest base; cannot load symbol filters.")
            return

        for symbol in symbols:
            try:
                url = f"{base}/v5/market/instruments-info?symbol={symbol}"
                resp = await http.get(url, timeout=10)
                if resp.status_code != 200:
                    # try without query param (get list)
                    alt_url = f"{base}/v5/market/instruments-info"
                    resp = await http.get(alt_url, timeout=10)
                    if resp.status_code != 200:
                        logger.debug(f"Failed to fetch instrument info for {symbol}: {resp.status_code}")
                        continue
                data = resp.json()
                # try to find list in common shapes
                result = data.get("result") if isinstance(data, dict) else data
                items = []
                if isinstance(result, dict) and "list" in result:
                    items = result["list"]
                elif isinstance(result, list):
                    items = result
                elif isinstance(result, dict):
                    # maybe result is the instrument
                    items = [result]
                else:
                    items = []

                matched = None
                for it in items:
                    # common key name is 'symbol'
                    if not isinstance(it, dict):
                        continue
                    if it.get("symbol") == symbol or it.get("instrument_name") == symbol:
                        matched = it
                        break
                if matched is None and items:
                    # fallback to first
                    matched = items[0]

                if not matched:
                    continue

                # try to extract min qty and step size in a robust way
                min_qty = None
                step_size = None

                # common nested filters
                possible_containers = ["lotSizeFilter", "lot_size_filter", "base_currency_precision", "filters", "size_filter"]
                for c in possible_containers:
                    if c in matched and isinstance(matched[c], dict):
                        cont = matched[c]
                        for cand in ("minOrderQuantity", "min_trd_qty", "minQty", "min_qty", "minSize", "minOrderQty", "minPrice", "minSize"):
                            if cand in cont:
                                try:
                                    min_qty = float(cont[cand])
                                    break
                                except Exception:
                                    pass
                        for cand in ("quantityStep", "qtyStep", "stepSize", "step_size", "quantity_step", "step"):
                            if cand in cont:
                                try:
                                    step_size = float(cont[cand])
                                    break
                                except Exception:
                                    pass
                        if min_qty is not None or step_size is not None:
                            break

                # try top-level keys as fallback
                if min_qty is None:
                    for cand in ("minOrderQty", "min_order_qty", "min_trd_qty", "minQty", "min_qty"):
                        if cand in matched:
                            try:
                                min_qty = float(matched[cand])
                                break
                            except Exception:
                                pass
                if step_size is None:
                    for cand in ("qtyStep", "quantityStep", "stepSize", "step_size"):
                        if cand in matched:
                            try:
                                step_size = float(matched[cand])
                                break
                            except Exception:
                                pass

                # as last resort: some endpoints include precision rather than step; we skip conversion
                if min_qty is None:
                    # try to use 'tick_size' like fields (not ideal)
                    min_qty = None
                if step_size is None:
                    step_size = None

                if min_qty is not None or step_size is not None:
                    logger.info(f"Updating symbol filters for {symbol}: min_qty={min_qty} step={step_size}")
                    self.update_symbol_filters(symbol, min_qty=min_qty, step_size=step_size)
                else:
                    logger.debug(f"No filters found for {symbol}, leaving defaults.")

            except Exception as e:
                logger.debug(f"Error loading filters for {symbol}: {e}")

    def update_symbol_filters(self, symbol: str, min_qty: Optional[float] = None, step_size: Optional[float] = None):
        if min_qty is not None:
            self.cfg.min_qty_defaults[symbol] = float(min_qty)
        if step_size is not None:
            self.cfg.step_size_defaults[symbol] = float(step_size)
        logger.debug(f"Symbol filters updated: {symbol} min_qty={min_qty} step={step_size}")
