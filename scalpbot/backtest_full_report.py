import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from binance.client import Client
import asyncio
import os

# ================== НАСТРОЙКИ ==================
API_KEY = ""  # оставь пустым, если не хочешь реальный ключ
API_SECRET = ""
SYMBOL = "BTCUSDT"
INITIAL_BALANCE = 10_000
DATA_FILE = "data/btc_yesterday.csv"

from strategy import Strategy  # твой файл со стратегиями
# ===============================================

# ---------- Шаг 1. Получаем данные с Binance ----------
def download_yesterday_data():
    client = Client(API_KEY, API_SECRET)

    end_time = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(days=1)

    start_str = start_time.strftime("%d %b, %Y %H:%M:%S")
    end_str = end_time.strftime("%d %b, %Y %H:%M:%S")

    candles = client.get_historical_klines(
        SYMBOL, Client.KLINE_INTERVAL_1MINUTE, start_str, end_str
    )

    df = pd.DataFrame(
        candles,
        columns=[
            "timestamp", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "number_of_trades",
            "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
        ]
    )

    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    df.set_index("timestamp", inplace=True)
    df[["open", "high", "low", "close", "volume"]] = df[
        ["open", "high", "low", "close", "volume"]
    ].apply(pd.to_numeric)

    os.makedirs("data", exist_ok=True)
    df.to_csv(DATA_FILE)
    return df


# ---------- Шаг 2. Подготовка клиента для эмуляции ----------
class MockClient:
    def __init__(self, df):
        self.df = df
        self.index = 0

    async def get_signal(self):
        """Эмулируем поступление новых данных"""
        if self.index >= len(self.df):
            return None
        row = self.df.iloc[self.index]
        self.index += 1
        return {"topic": SYMBOL, "trade": {"price": row["close"], "size": 0.01}}

    async def place_order(self, symbol, side, qty, price=None):
        executed_price = price or self.df.iloc[self.index - 1]["close"]
        print(f"[EXECUTED] {side} {symbol} {qty} @ {executed_price}")
        return {"status": "filled", "price": executed_price}


# ---------- Шаг 3. Запуск стратегии ----------
async def run_backtest(df):
    mock_client = MockClient(df)
    strategy = Strategy(symbols=[SYMBOL])
    strategy.set_balance(SYMBOL, INITIAL_BALANCE)

    while True:
        signal = await mock_client.get_signal()
        if signal is None:
            break
        await strategy.handle_signal(signal)

    return strategy


# ---------- Шаг 4. Отчётность и визуализация ----------
def plot_results(balance_history, trade_log):
    plt.figure(figsize=(12, 6))

    # Баланс
    plt.subplot(2, 1, 1)
    plt.plot(balance_history, label="Balance")
    plt.title("Equity Curve")
    plt.legend()

    # Сделки
    plt.subplot(2, 1, 2)
    wins = [t["pnl"] for t in trade_log if t["pnl"] > 0]
    losses = [t["pnl"] for t in trade_log if t["pnl"] <= 0]

    plt.hist([wins, losses], bins=30, stacked=True, label=["Wins", "Losses"])
    plt.title("PnL Distribution")
    plt.legend()

    plt.tight_layout()
    plt.show()


# ---------- Шаг 5. Главный запуск ----------
if __name__ == "__main__":
    # Загружаем данные
    if os.path.exists(DATA_FILE):
        df = pd.read_csv(DATA_FILE, index_col="timestamp", parse_dates=True)
    else:
        df = download_yesterday_data()

    # Запускаем стратегию
    strategy = asyncio.run(run_backtest(df))

    # Итоги
    perf = strategy.strategy_performance.get(SYMBOL, {})
    balance = perf.get("balance", INITIAL_BALANCE)
    trade_log = perf.get("trades", [])

    print("\n=== Backtest Report ===")
    print(f"Symbol: {SYMBOL}")
    print(f"Initial Balance: {INITIAL_BALANCE}")
    print(f"Final Balance:   {balance}")
    print(f"Total Trades:    {len(trade_log)}")

    wins = sum(1 for t in trade_log if t["pnl"] > 0)
    losses = sum(1 for t in trade_log if t["pnl"] <= 0)
    print(f"Wins: {wins} | Losses: {losses}")
    if len(trade_log) > 0:
        avg_pnl = sum(t["pnl"] for t in trade_log) / len(trade_log)
        print(f"Avg PnL: {avg_pnl:.2f}")

    # График
    balance_history = [INITIAL_BALANCE]
    running_balance = INITIAL_BALANCE
    for t in trade_log:
        running_balance += t["pnl"]
        balance_history.append(running_balance)

    plot_results(balance_history, trade_log)
