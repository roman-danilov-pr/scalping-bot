# scalpbot/shared_state.py
import threading
import time
from collections import deque
from typing import Dict, Any, List

_lock = threading.RLock()  # рекурсивный lock для безопасного вызова внутри функций

class SharedState:
    def __init__(self):
        # Фичи для каждой пары: цена, объем, индикаторы
        self.features: Dict[str, Dict[str, Any]] = {}

        # Сигналы торговли
        self.signals: deque = deque(maxlen=1000)

        # Лог бота
        self.logs: deque = deque(maxlen=5000)

        # Открытые позиции
        self.open_positions: Dict[int, Dict[str, Any]] = {}

        # Закрытые позиции
        self.closed_positions: List[Dict[str, Any]] = []

        # Метрики (PnL, uptime, ROI и т.д.)
        self.metrics: Dict[str, Any] = {}

        # Стартовое время бота
        self.start_ts = time.time()

        # Генератор уникальных ID ордеров
        self.order_id_counter = 1

    # ------------------ Features ------------------
    def update_feature(self, symbol: str, feat: Dict[str, Any]):
        with _lock:
            if symbol not in self.features:
                self.features[symbol] = {}
            self.features[symbol].update(feat)

    def get_features(self, symbol: str) -> Dict[str, Any]:
        with _lock:
            return self.features.get(symbol, {}).copy()

    # ------------------ Signals ------------------
    def push_signal(self, sig: Dict[str, Any]):
        with _lock:
            sig['ts'] = time.time()
            self.signals.append(sig)

    def get_signals(self, n: int = 200) -> List[Dict[str, Any]]:
        with _lock:
            return list(self.signals)[-n:]

    # ------------------ Logs ------------------
    def push_log(self, text: str):
        with _lock:
            self.logs.append({'ts': time.time(), 'text': text})
            print(f"[LOG] {text}")  # для дебага в консоли

    def tail_logs(self, n: int = 200) -> List[Dict[str, Any]]:
        with _lock:
            return list(self.logs)[-n:]

    # ------------------ Positions ------------------
    def open_pos(self, pos: Dict[str, Any]) -> int:
        with _lock:
            pos_id = self.order_id_counter
            self.order_id_counter += 1
            pos['id'] = pos_id
            pos['status'] = 'open'
            pos['open_ts'] = time.time()
            self.open_positions[pos_id] = pos
            self.push_log(f"Opened position {pos_id}: {pos}")
            return pos_id

    def close_pos(self, pos_id: int, closed_info: Dict[str, Any]):
        with _lock:
            pos = self.open_positions.pop(pos_id, None)
            if pos:
                pos.update(closed_info)
                pos['status'] = 'closed'
                pos['closed_ts'] = time.time()
                self.closed_positions.append(pos)
                self.push_log(f"Closed position {pos_id}: {pos}")

    def get_open_positions(self) -> List[Dict[str, Any]]:
        with _lock:
            return list(self.open_positions.values())

    def get_closed_positions(self, n: int = 100) -> List[Dict[str, Any]]:
        with _lock:
            return self.closed_positions[-n:]

    # ------------------ Metrics ------------------
    def update_metric(self, key: str, value: Any):
        with _lock:
            self.metrics[key] = value

    def get_metrics(self) -> Dict[str, Any]:
        with _lock:
            return self.metrics.copy()

    # ------------------ Snapshot для UI / API ------------------
    def snapshot(self) -> Dict[str, Any]:
        with _lock:
            return {
                'features': dict(self.features),
                'signals': list(self.signals),
                'open_positions': list(self.open_positions.values()),
                'closed_positions': list(self.closed_positions),
                'metrics': dict(self.metrics),
                'logs': list(self.logs)[-200:],
                'uptime': time.time() - self.start_ts,
            }

# Singleton для всего бота
_shared_state = SharedState()

def get_state() -> SharedState:
    return _shared_state
