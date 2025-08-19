# scalpbot/test_runner.py
"""
Test runner to wire Strategy + PaperExecutor + SQLiteEventSink.
Use as package module:
    python -m scalpbot.test_runner --mode mock --time 60 --start-balance 10000
or
    python -m scalpbot.test_runner --mode live --time 300
"""

import asyncio
import logging
import random
import signal
import argparse
import sqlite3
import csv
from datetime import datetime, timedelta
from typing import List, Optional

# package-relative imports (works when running `python -m scalpbot.test_runner`)
from .strategy import Strategy
from .paper_executor import PaperExecutor
from .event_sinks import SQLiteEventSink

# optional: import your real BybitClient if you want live mode
try:
    from .bybit_client import BybitClient
except Exception:
    BybitClient = None

logger = logging.getLogger("scalpbot.test_runner")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


# -------- Mock feed --------
class MockFeed:
    """Generates synthetic ticks for symbols at approx real-time pace.

    Also supports get_balance coroutine auto-attached by runner (see below).
    """
    def __init__(self, symbols: List[str], start_price: float = 1000.0, drift: float = 0.0):
        self.symbols = symbols
        self._last_price = {s: start_price for s in symbols}
        self._running = True

    async def start(self):
        self._running = True

    async def stop(self):
        self._running = False

    async def get_signal(self):
        # small sleep to simulate tick cadence
        await asyncio.sleep(random.uniform(0.05, 0.25))
        s = random.choice(self.symbols)
        last = self._last_price[s]
        step = random.uniform(-0.0025, 0.0025) * last
        price = max(0.0001, last + step)
        self._last_price[s] = price
        tick = {
            "topic": f"publicTrade.{s}",
            "trade": {"symbol": s, "ts": datetime.utcnow().timestamp(), "price": price, "vol": random.uniform(0.0001, 0.01), "side": random.choice(["Buy", "Sell"])}
        }
        return tick

    # optional helper used by PaperExecutor (paper executor will check feed._last_price)
    @property
    def last_price(self):
        return dict(self._last_price)


# ------------ runner ------------
async def run_test(mode: str = "mock", run_seconds: int = 60, start_balance: float = 10000.0, db_path: str = "paper_trades.db"):
    symbols = ["BTCUSDT", "ETHUSDT"]

    # choose feed
    if mode == "live" and BybitClient:
        feed = BybitClient(symbols=symbols, paper=True, initial_balance=start_balance)
        # connect if needed
        logger.info("Connecting BybitClient (live-mode, paper) ...")
        await feed.connect()
    else:
        feed = MockFeed(symbols=symbols, start_price=1000.0)
        # attach a get_balance coroutine so RiskManager has a balance to work with
        async def _mock_get_balance():
            return {"USDT": float(start_balance)}
        feed.get_balance = _mock_get_balance  # dynamic attach
        # also expose _last_price property name expected by executor
        feed._last_price = feed._last_price

    # event sink -> SQLite db
    sink = SQLiteEventSink(path=db_path)

    # paper executor
    executor = PaperExecutor(feed=feed, commission_rate=0.0006, slippage_rate=0.0005, event_sink=sink)

    # build strategy (it will create RiskManager if not provided)
    strat = Strategy(feed=feed, symbols=symbols, window=60, executor=executor, event_sink=sink, max_trade_time=timedelta(minutes=60))

    # start strategy task
    strat_task = asyncio.create_task(strat.run())

    logger.info(f"Test started in mode={mode}, duration={run_seconds}s, start_balance={start_balance}. DB={db_path}")

    stop_time = asyncio.get_event_loop().time() + run_seconds
    try:
        while asyncio.get_event_loop().time() < stop_time:
            await asyncio.sleep(1.0)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt -> scheduling shutdown")

    # stop strategy
    strat.stop()
    await asyncio.sleep(0.2)

    # ensure strategy task completes
    try:
        await asyncio.wait_for(strat_task, timeout=5.0)
    except asyncio.TimeoutError:
        strat_task.cancel()
        try:
            await strat_task
        except Exception:
            pass

    # close sink
    try:
        sink.close()
    except Exception:
        pass

    # if live feed — disconnect
    if mode == "live" and BybitClient:
        try:
            await feed.disconnect()
        except Exception:
            pass

    # after run: compute summary & export csv
    try:
        _summarize_and_export(db_path)
    except Exception as e:
        logger.exception(f"Failed to summarize/export DB: {e}")

    logger.info("Test run finished.")


def _summarize_and_export(db_path: str = "paper_trades.db", csv_path: str = "trades_export.csv"):
    """Compute simple stats from trades table and export CSV."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # ensure table exists
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trades'")
    if not cur.fetchone():
        logger.info("No trades table found in DB; nothing to summarize.")
        conn.close()
        return

    # total trades
    cur.execute("SELECT COUNT(*) FROM trades")
    total = cur.fetchone()[0] or 0

    # sum pnl_net
    cur.execute("SELECT COALESCE(SUM(pnl_net),0) FROM trades")
    sum_pnl = cur.fetchone()[0] or 0.0

    # winrate
    cur.execute("SELECT SUM(CASE WHEN pnl_net>0 THEN 1 ELSE 0 END), COUNT(*) FROM trades")
    wins = cur.fetchone()[0] or 0
    total2 = cur.fetchone()[1] if False else None  # fallback, we'll just use 'total' above
    winrate = (wins / total) if total > 0 else 0.0

    # avg pnl
    cur.execute("SELECT COALESCE(AVG(pnl_net),0) FROM trades")
    avg_pnl = cur.fetchone()[0] or 0.0

    # max drawdown rudimentary (compute equity curve)
    cur.execute("SELECT exit_ts, COALESCE(pnl_net,0) FROM trades ORDER BY exit_ts ASC")
    rows = cur.fetchall()
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for r in rows:
        equity += (r[1] or 0.0)
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd

    # print summary
    logger.info("=== Test summary ===")
    logger.info(f"Total trades: {total}")
    logger.info(f"Sum PnL (net): {sum_pnl:.6f}")
    logger.info(f"Wins: {wins}, Winrate: {winrate:.2%}")
    logger.info(f"Average PnL per trade: {avg_pnl:.6f}")
    logger.info(f"Max drawdown (rudimentary): {max_dd:.6f}")

    # export trades table to CSV
    cur.execute("SELECT * FROM trades")
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    try:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(cols)
            w.writerows(rows)
        logger.info(f"Exported trades to {csv_path}")
    except Exception as e:
        logger.exception(f"Failed to write CSV: {e}")

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["mock", "live"], default="mock", help="mock or live mode")
    parser.add_argument("--time", type=int, default=60, help="run time seconds")
    parser.add_argument("--start-balance", type=float, default=10000.0, help="starting balance for mock (USDT)")
    parser.add_argument("--db", type=str, default="paper_trades.db", help="sqlite db path")
    args = parser.parse_args()

    asyncio.run(run_test(mode=args.mode, run_seconds=args.time, start_balance=args.start_balance, db_path=args.db))
