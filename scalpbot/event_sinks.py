# scalpbot/event_sinks.py
"""
Event sinks: store events (strategy events, executions, opens/closes) into SQLite and optionally CSV.

- SQLite schema: events (ts REAL, type TEXT, symbol TEXT, payload JSON)
- Helper methods automatically insert position_opened/position_closed into a trades table for convenient queries.
"""

import sqlite3
import json
import os
from typing import Dict, Any, Optional
from datetime import datetime
import logging

logger = logging.getLogger("scalpbot.event_sinks")


class SQLiteEventSink:
    def __init__(self, path: str = "paper_trades.db", append: bool = True):
        self.path = path
        # ensure directory
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self._ensure_schema()

    def _ensure_schema(self):
        cur = self.conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL,
            type TEXT,
            symbol TEXT,
            payload TEXT
        )""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            side TEXT,
            entry_ts REAL,
            entry_price REAL,
            exit_ts REAL,
            exit_price REAL,
            qty REAL,
            pnl_gross REAL,
            commission REAL,
            pnl_net REAL,
            reason TEXT
        )""")
        self.conn.commit()

    def __call__(self, event: Dict[str, Any]):
        # sync handler (strategy may call sync or async)
        try:
            t = float(event.get("ts", datetime.utcnow().timestamp()))
            typ = event.get("type", "")
            sym = event.get("symbol")
            payload = dict(event)
            payload.pop("ts", None)
            payload.pop("type", None)
            payload.pop("symbol", None)
            payload_json = json.dumps(payload, default=str, ensure_ascii=False)

            cur = self.conn.cursor()
            cur.execute("INSERT INTO events (ts, type, symbol, payload) VALUES (?, ?, ?, ?)",
                        (t, typ, sym, payload_json))

            # if position_closed event -> also insert into trades table (for easy analysis)
            if typ == "position_closed":
                # payload expected to have entry_price, executed_price, qty, commission, pnl_gross, pnl_net, reason
                entry_price = float(payload.get("entry_price") or 0.0)
                executed_price = float(payload.get("executed_price") or 0.0)
                qty = float(payload.get("qty") or 0.0)
                commission = float(payload.get("commission") or 0.0)
                pnl_gross = float(payload.get("pnl_gross") or 0.0)
                pnl_net = float(payload.get("pnl_net") or 0.0)
                reason = str(payload.get("reason") or "")
                entry_ts = float(payload.get("entry_ts") or t)
                exit_ts = t
                side = payload.get("side") or "?"
                cur.execute("""
                    INSERT INTO trades (symbol, side, entry_ts, entry_price, exit_ts, exit_price, qty, pnl_gross, commission, pnl_net, reason)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (sym, side, entry_ts, entry_price, exit_ts, executed_price, qty, pnl_gross, commission, pnl_net, reason))
            self.conn.commit()
        except Exception as e:
            logger.exception(f"SQLiteEventSink insert failed: {e}")

    def close(self):
        try:
            self.conn.commit()
            self.conn.close()
        except Exception:
            pass
