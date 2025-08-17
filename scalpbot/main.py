# scalpbot/main.py
import asyncio
import logging
import signal
import sys

from bybit_client import BybitClient
from strategy import Strategy
from risk_manager import RiskManager
from shared_state import get_state
from config import get_config

# ------------------ Настройка логирования ------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("scalpbot")
state = get_state()


async def main():
    cfg = get_config()

    SYMBOLS = list(cfg.symbols)
    WINDOW = cfg.window
    PAPER_MODE = cfg.paper_mode

    logger.info(f"Starting scalpbot. Symbols={SYMBOLS}, window={WINDOW}, paper={PAPER_MODE}")
    state.push_log(f"Scalpbot starting. Paper mode: {PAPER_MODE}")

    # --- Инициализация клиента Bybit ---
    client = BybitClient(symbols=SYMBOLS, paper=PAPER_MODE, initial_balance=cfg.initial_balance)
    try:
        if getattr(cfg, "ws_url", None):
            client.ws_url = cfg.ws_url
        if getattr(cfg, "rest_url", None):
            client.rest_base = cfg.rest_url
    except Exception:
        pass

    # connect WS
    connect_task = asyncio.create_task(client.connect())

    # --- Инициализация RiskManager и загрузка фильтров симолов ---
    risk_manager = RiskManager(client)
    # populate daily loss pct from config if present
    try:
        if getattr(cfg, "max_daily_loss_pct", None) is not None:
            risk_manager.cfg.daily_loss_limit_pct = float(cfg.max_daily_loss_pct)
    except Exception:
        pass

    logger.info("Loading symbol filters from exchange (best-effort)...")
    try:
        await risk_manager.load_symbol_filters_from_exchange(SYMBOLS)
    except Exception as e:
        logger.warning(f"Failed to load symbol filters: {e}")

    # --- Инициализация стратегии, передаём RiskManager ---
    strategy = Strategy(client, SYMBOLS, window=WINDOW, risk_manager=risk_manager)
    strategy_task = asyncio.create_task(strategy.run())

    # --- Корректное завершение ---
    loop = asyncio.get_running_loop()
    shutdown_in_progress = False

    def _schedule_shutdown():
        nonlocal shutdown_in_progress
        if shutdown_in_progress:
            logger.info("Shutdown already in progress")
            return
        shutdown_in_progress = True
        logger.info("Scheduling shutdown: stopping strategy and client...")
        state.push_log("Scheduling shutdown: stopping strategy and client...")
        try:
            strategy.stop()
        except Exception:
            try:
                loop.create_task(strategy.stop_async())
            except Exception:
                pass

        async def _shutdown():
            try:
                await client.disconnect()
            except Exception as e:
                logger.exception(f"Error during client.disconnect: {e}")

            try:
                await asyncio.wait_for(strategy_task, timeout=10.0)
            except asyncio.TimeoutError:
                logger.warning("Strategy did not stop within timeout, cancelling task")
                try:
                    strategy_task.cancel()
                except Exception:
                    pass
            except Exception as e:
                logger.exception(f"Error waiting for strategy task: {e}")

            try:
                if not connect_task.done():
                    connect_task.cancel()
            except Exception:
                pass

        loop.create_task(_shutdown())

    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _schedule_shutdown)
            except Exception:
                pass
    else:
        logger.info("Signal handlers skipped on Windows. Use Ctrl+C to stop.")

    try:
        await strategy_task
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Scheduling shutdown...")
        state.push_log("KeyboardInterrupt received. Scheduling shutdown...")
        _schedule_shutdown()
        try:
            await asyncio.wait_for(strategy_task, timeout=10.0)
        except Exception:
            pass
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass

    logger.info("Scalpbot stopped.")
    state.push_log("Scalpbot stopped.")


if __name__ == "__main__":
    asyncio.run(main())
