import asyncio
import logging
import sys
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from binance import AsyncClient, BinanceSocketManager

from config import settings
from notifications import Telegram

logger = logging.getLogger(__name__)


async def main(symbol: str = "BTCUSDT", notifier: Telegram | None = None):
    # Configuration parameters
    MIN_BASIS = 0.0024  # 0.24%
    SAFETY_MARGIN = 0.0004  # 0.04%
    MIN_FUNDING_RATE = 0.0  # funding must be positive
    ENTRY_THRESHOLD = MIN_BASIS + SAFETY_MARGIN  # 0.28%

    # Shared state for latest prices and funding rate
    state = {
        "spot_price": None,
        "futures_price": None,
        "funding_rate": None,
        "last_signal": None,  # Track last signal to avoid duplicate logs
        "last_basis": None,  # Track last basis (3 decimal accuracy)
        "last_funding": None,  # Track last funding rate (4 decimal accuracy)
        "last_all_conditions_met": False,  # Track if all conditions were met
    }

    logging.info("Starting Binance Spot-Futures Arbitrage Basis Monitor...")
    if notifier:
        await notifier.text("Starting Binance Spot-Futures Arbitrage Basis Monitor...")

    logging.info(
        f"Configuration: MIN_BASIS={MIN_BASIS*100:.2f}%, SAFETY_MARGIN={SAFETY_MARGIN*100:.2f}%, ENTRY_THRESHOLD={ENTRY_THRESHOLD*100:.2f}%"
    )

    client = await AsyncClient.create()
    # Increase queue size to handle high-frequency market data streams
    # Default is 100, but we need more for busy markets like BTCUSDT
    # Using 1000 provides adequate buffer for message bursts
    bsm = BinanceSocketManager(client, max_queue_size=1000)

    async def handle_spot_trade(msg):
        """Handle spot trade stream updates - non-blocking"""
        try:
            # Extract data from nested structure
            data = msg.get("data", msg)
            # Get spot price from trade stream (just update state, don't log)
            state["spot_price"] = float(data["p"])
        except KeyError as e:
            logger.error(f"Spot trade message missing key {e}: {msg}")
        except Exception as e:
            logger.error(f"Error handling spot trade: {e}", exc_info=True)

    async def handle_futures_mark_price(msg):
        """Handle futures mark price stream updates - non-blocking"""
        try:
            # Extract data from nested structure
            data = msg.get("data", msg)
            # Get futures mark price
            state["futures_price"] = float(data["p"])
            # Get funding rate from mark price stream
            state["funding_rate"] = float(data["r"])
        except KeyError as e:
            logger.error(f"Futures mark price message missing key {e}: {msg}")
        except Exception as e:
            logger.error(f"Error handling futures mark price: {e}", exc_info=True)

    async def evaluate_signal_loop():
        """Periodically evaluate and log arbitrage signals"""
        while True:
            try:
                spot = state["spot_price"]
                futures = state["futures_price"]
                funding = state["funding_rate"]

                # Wait until we have all data
                if spot is None or futures is None or funding is None:
                    await asyncio.sleep(0.5)
                    continue

                # Calculate basis
                basis = calculate_basis(spot, futures)
                if basis is None:
                    await asyncio.sleep(0.5)
                    continue

                # Evaluate conditions
                conditions = {
                    "positive_basis": futures > spot,
                    "basis_threshold": basis >= ENTRY_THRESHOLD,
                    "funding_positive": funding > MIN_FUNDING_RATE,
                }

                # Determine signal
                all_conditions_met = all(conditions.values())

                # Format output
                signal_status = "ENTER ✅" if all_conditions_met else "WAIT ❌"

                # Build reason if not entering
                reasons = []
                if not conditions["positive_basis"]:
                    reasons.append("Negative basis")
                if not conditions["basis_threshold"]:
                    reasons.append("Basis too small")
                if not conditions["funding_positive"]:
                    reasons.append("Funding unfavorable")

                reason_text = f" ({', '.join(reasons)})" if reasons else ""

                # Create current signal snapshot
                current_signal = (signal_status, reason_text)

                # Round basis and funding to respective decimal places for comparison
                basis_rounded = round(basis * 100, 3)  # Convert to percentage and round
                funding_rounded = round(funding * 100, 4)  # Convert to percentage and round

                # Rate limiting: Only log if signal changed OR basis changed (3 decimals) OR funding changed (4 decimals)
                should_log = (
                    current_signal != state["last_signal"]
                    or basis_rounded != state["last_basis"]
                    or funding_rounded != state["last_funding"]
                )

                if should_log:
                    # Log the signal
                    logger.info(
                        f"{symbol} | "
                        f"Spot: {spot:,.2f} | Futures: {futures:,.2f} | "
                        f"Basis: {basis*100:.3f}% | Funding: {funding*100:.4f}% | "
                        f"{signal_status}{reason_text}"
                    )

                if notifier:
                    should_notify = all_conditions_met != state["last_all_conditions_met"]

                    if should_notify:
                        await notifier.text(
                            f"Arbitrage Signal Detected:\n"
                            f"{symbol} | "
                            f"Spot: {spot:,.2f} | Futures: {futures:,.2f} | "
                            f"Basis: {basis*100:.3f}% | Funding: {funding*100:.4f}% | "
                            f"{signal_status}{reason_text}"
                        )

                state["last_signal"] = current_signal
                state["last_basis"] = basis_rounded
                state["last_funding"] = funding_rounded
                state["last_all_conditions_met"] = all_conditions_met

            except Exception as e:
                logger.error(f"Error in evaluate_signal_loop: {e}", exc_info=True)

            # Check every 0.5 seconds
            await asyncio.sleep(0.5)

    # Start WebSocket streams with automatic reconnection
    max_retries = None  # Retry indefinitely
    retry_count = 0
    retry_delay = 5  # seconds between retries

    while max_retries is None or retry_count < max_retries:
        try:
            # Use aggregated trade stream instead of raw trades to reduce message volume
            # Aggregated trades update every 100ms, much less frequent than individual trades
            spot_stream = bsm.aggtrade_socket(symbol)
            # Futures mark price stream
            futures_stream = bsm.symbol_mark_price_socket(symbol)

            # Create tasks for both streams
            async with spot_stream as spot_ws, futures_stream as futures_ws:
                logger.info(
                    f"WebSocket streams connected (attempt {retry_count + 1}). Monitoring for arbitrage signals..."
                )
                if retry_count > 0 and notifier:
                    await notifier.text(f"WebSocket reconnected after {retry_count} attempts")

                # Reset retry counter on successful connection
                retry_count = 0

                # Process messages concurrently
                spot_task = asyncio.create_task(
                    process_stream(spot_ws, handle_spot_trade, "Spot Aggregated Trade")
                )
                futures_task = asyncio.create_task(
                    process_stream(futures_ws, handle_futures_mark_price, "Futures Mark Price")
                )
                # Start signal evaluation loop
                eval_task = asyncio.create_task(evaluate_signal_loop())

                # Wait for all tasks
                await asyncio.gather(spot_task, futures_task, eval_task)

        except KeyboardInterrupt:
            logger.info("Shutting down gracefully...")
            break
        except Exception as e:
            retry_count += 1
            logger.error(
                f"Error in main loop (attempt {retry_count}): {e}",
                exc_info=True,
            )

            if notifier:
                await notifier.text(
                    f"WebSocket error detected. Reconnecting in {retry_delay}s... (attempt {retry_count})"
                )

            # Wait before retrying
            logger.info(f"Reconnecting in {retry_delay} seconds...")
            await asyncio.sleep(retry_delay)

            # Exponential backoff with max delay of 60 seconds
            retry_delay = min(retry_delay * 1.5, 60)

    # Cleanup
    await client.close_connection()
    if notifier:
        await notifier.text("Binance Spot-Futures Arbitrage Monitor terminated")
        await notifier.close()


async def process_stream(stream, handler, stream_name):
    """Process WebSocket stream messages - drain queue as fast as possible"""
    try:
        while True:
            msg = await stream.recv()
            # Process message without await to avoid blocking
            # Handler functions are already async but they don't await anything
            # So we can call them synchronously for maximum speed
            try:
                # Call handler directly (handlers are very fast, just update state)
                if asyncio.iscoroutinefunction(handler):
                    await handler(msg)
                else:
                    handler(msg)
            except Exception as e:
                logger.error(f"Error in {stream_name} handler: {e}")
    except Exception as e:
        logger.error(f"{stream_name} stream error: {e}", exc_info=True)
        raise


def calculate_basis(spot_price, futures_price):
    if spot_price and futures_price:
        return (futures_price - spot_price) / spot_price
    return None


def setup_logging() -> None:
    """Configure logging system based on settings.

    Args:
        settings (Settings): Configuration containing log level and format preferences
    """
    # Create log directory if it doesn't exist
    log_file = Path("log/app.log")
    log_file.parent.mkdir(parents=True, exist_ok=True)

    # Create a timed rotating file handler that rotates daily at midnight
    file_handler = TimedRotatingFileHandler(
        filename=str(log_file),
        when="midnight",  # Rotate at midnight
        interval=1,  # Rotate every 1 day
        backupCount=7,  # Keep 7 backup files (7 days of logs)
        encoding="utf-8",  # Use UTF-8 encoding for log files
    )

    # Set the suffix for rotated files (adds date to filename)
    file_handler.suffix = "%Y-%m-%d"

    # Create console handler for stdout output with UTF-8 encoding
    # Reconfigure stdout to use UTF-8 for Windows compatibility
    if sys.stdout.encoding != "utf-8":
        import io

        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    console_handler = logging.StreamHandler(sys.stdout)

    # Configure root logger
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)20.20s][%(funcName)20.20s][%(levelname)5.5s] %(message)s",
        handlers=[file_handler, console_handler],
    )


if __name__ == "__main__":
    setup_logging()
    symbol = "BTCUSDT"

    token = settings.TELEGRAM_BOT_TOKEN
    chat_id = settings.TELEGRAM_CHAT_ID
    notifier = Telegram(token=token, chat_id=chat_id) if token and chat_id else None

    if notifier:
        logger.info("Telegram notifier configured")
    else:
        logger.info("Telegram notifier NOT configured")

    asyncio.run(main(symbol, notifier))
