# test_strategy_improved.py
import asyncio
import random
from datetime import datetime, timedelta, timezone
from collections import defaultdict, deque
from scalpbot.strategy import Strategy

class DummyClient:
    def __init__(self, symbols):
        self.symbols = symbols
        self.price = {s: 50000 for s in symbols}  # стартовая цена
        self.virtual_balance = 500.0  # USDT

    async def get_signal(self):
        await asyncio.sleep(0.05)
        symbol = random.choice(self.symbols)
        change = random.uniform(-1, 1)
        self.price[symbol] *= (1 + change / 100)
        return {
            "topic": f"trade.{symbol}",
            "trade": {"price": round(self.price[symbol], 2), "size": 0.01}
        }

    async def place_order(self, symbol, side, qty, price=None):
        price = price or self.price[symbol]
        cost = qty * price
        if side == "Buy" and self.virtual_balance >= cost:
            self.virtual_balance -= cost
            return {"status": "filled", "price": price}
        elif side == "Sell":
            self.virtual_balance += cost
            return {"status": "filled", "price": price}
        return {"status": "rejected", "price": price}


async def run_test():
    symbols = ["BTCUSD"]
    client = DummyClient(symbols)
    strategy = Strategy(client, symbols, window=20)

    # ---------------- Статистика ----------------
    stats = {
        "total_signals": 0,
        "total_orders": 0,
        "filled_orders": 0,
        "rejected_orders": 0,
        "profit_loss": 0.0,
        "strategy_stats": defaultdict(lambda: {"filled": 0, "rejected": 0, "pnl": 0.0})
    }

    # ---------------- Переопределяем _safe_place_order для сбора статистики ----------------
    async def safe_place_order(symbol, side, qty, price=None, strategy_name="test_strategy"):
        price = price or client.price[symbol]
        pos = strategy.open_positions.get(symbol)
        pnl = 0.0
        if pos and "price" in pos:
            entry_price = pos["price"]
            if pos["side"] == "Buy":
                pnl = (price - entry_price) * pos.get("qty", 0)
            else:
                pnl = (entry_price - price) * pos.get("qty", 0)

        # Вызов реального place_order у Strategy
        result = await strategy._place_order(symbol, side, qty, price)

        # ---------------- Проверка на None ----------------
        if result is None:
            result = {"status": "error", "price": price}

        # Обновление статистики
        stats["total_orders"] += 1
        stats["profit_loss"] += pnl
        strat_stat = stats["strategy_stats"][strategy_name]
        strat_stat["pnl"] += pnl

        if result.get("status") == "filled":
            stats["filled_orders"] += 1
            strat_stat["filled"] += 1
        else:
            stats["rejected_orders"] += 1
            strat_stat["rejected"] += 1

        now = datetime.now(timezone.utc).strftime("%H:%M:%S")
        print(f"[{now}] {strategy_name}: {side} {symbol} {qty} @ {price} -> {result.get('status')} | "
              f"PnL: {round(pnl, 2)} | Balance: {round(client.virtual_balance, 2)}")
        return result

    strategy._safe_place_order = safe_place_order

    # ---------------- Основной цикл теста ----------------
    start_time = datetime.now(timezone.utc)
    print(f"--- Test start at {start_time} ---")
    test_duration = timedelta(seconds=60)

    while datetime.now(timezone.utc) - start_time < test_duration:
        signal = await client.get_signal()
        stats["total_signals"] += 1
        await strategy._process_signal(signal)
        await strategy._monitor_positions()

    # ---------------- Закрытие всех позиций ----------------
    for symbol, pos in strategy.open_positions.items():
        if pos and "qty" in pos and "side" in pos:
            price = client.price[symbol]
            side = "Sell" if pos["side"] == "Buy" else "Buy"
            qty = pos["qty"]
            await strategy._place_order(symbol, side, qty)

    end_time = datetime.now(timezone.utc)
    print(f"--- Test end at {end_time} ---")
    print("\n=== TEST SUMMARY ===")
    print(f"Total signals processed: {stats['total_signals']}")
    print(f"Total orders: {stats['total_orders']} (Filled: {stats['filled_orders']}, Rejected: {stats['rejected_orders']})")
    print(f"Net PnL: {round(stats['profit_loss'],2)} USDT")
    print(f"Final virtual balance: {round(client.virtual_balance,2)} USDT\n")

    print("Strategy-specific stats:")
    for strat_name, s in stats["strategy_stats"].items():
        print(f" - {strat_name}: Filled={s['filled']}, Rejected={s['rejected']}, PnL={round(s['pnl'],2)} USDT")

    print(f"\nOpen positions at end: {strategy.open_positions}")


if __name__ == "__main__":
    asyncio.run(run_test())
