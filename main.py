"""Binance Spot-Futures Arbitrage Spread Monitor.

This module implements a real-time monitoring system for detecting spot-futures
arbitrage opportunities on Binance. It subscribes to WebSocket streams for spot
trades, futures aggregate trades, and funding rate updates to calculate spreads
and generate entry/exit signals.

Core Strategy:
    - Long Spot BTC + Short BTCUSDT Perpetual Futures
    - Entry signal requires:
        - Positive spread (futures > spot)
        - Spread exceeds minimum threshold + safety margin
        - Positive funding rate
    - Exit signal requires:
        - Spread <= EXIT_MAX_SPREAD threshold

State Machine:
    - NOT IN POSITION: Monitors for entry conditions
    - IN POSITION: Monitors for exit conditions (spread <= exit threshold)
    - Transitions automatically on entry/exit signal

Example:
    Run the monitor for configured symbols:

        python main.py

    Or programmatically:

        from main import main
        from notifications import Telegram

        notifier = Telegram(token="...", chat_id="...")
        await main(
            symbols=["BTCUSDT", "ETHUSDT"],
            min_spread=0.0024,
            safety_margin=0.0004,
            exit_max_spread=0.0,
            notifier=notifier,
        )

Attributes:
    logger: Module-level logger for main application events.
"""

import asyncio
import logging
import time
from decimal import Decimal

from binance import AsyncClient, BinanceSocketManager, ReconnectingWebsocket

from core.config import settings
from core.logging_config import setup_logging
from core.throttled_debouncer import ThrottledDebouncer
from notifications import Telegram

# Module-level logger for main application events
logger = logging.getLogger(__name__)


async def monitor_symbol(
    symbol: str,
    bsm: BinanceSocketManager,
    notifier: Telegram | None = None,
    min_spread: float = 0.0024,
    safety_margin: float = 0.0004,
    min_funding_rate: float = 0.0,
    exit_max_spread: float = 0.0,
) -> None:
    """Monitor a single trading pair for arbitrage opportunities.

    Subscribes to Binance WebSocket streams for spot trades, futures aggregate
    trades, and mark price updates (funding rate). Evaluates entry conditions
    on each price update and sends notifications when conditions change.

    Args:
        symbol: Trading pair symbol to monitor (e.g., "BTCUSDT").
        bsm: BinanceSocketManager instance for creating WebSocket connections.
        notifier: Optional Telegram notifier for sending alerts. If None,
            no notifications are sent.
        min_spread: Minimum spread percentage required for entry signal.
            Default is 0.24% (0.0024).
        safety_margin: Additional safety buffer added to min_spread.
            Default is 0.04% (0.0004).
        min_funding_rate: Minimum funding rate threshold. Entry signal
            requires funding rate > this value. Default is 0.0.
        exit_max_spread: Maximum spread threshold to trigger exit signal.
            Exit signal fires when spread <= this value. Default is 0.0 (0%).

    Returns:
        None. This function runs indefinitely until interrupted.

    Raises:
        KeyboardInterrupt: When user requests shutdown via Ctrl+C.
        Exception: WebSocket connection errors trigger automatic reconnection.

    Note:
        This function runs in an infinite loop with automatic reconnection.
        It will only exit on KeyboardInterrupt or after max_retries (if set).
    """
    # Calculate entry threshold by combining minimum spread and safety margin
    # This ensures we have enough profit margin after accounting for fees
    entry_threshold = Decimal(str(min_spread + safety_margin))

    # Exit threshold - when spread falls to this level, signal to close position
    exit_threshold = Decimal(str(exit_max_spread))

    # Asyncio lock for thread-safe state access across concurrent handlers
    # Prevents race conditions when multiple streams update state simultaneously
    state_lock = asyncio.Lock()

    # Shared state dictionary for tracking prices and signal conditions
    # All handlers update this state and evaluate_signal reads from it
    state = {
        # Current market prices from WebSocket streams
        "spot_price": None,  # Latest spot price from trade stream
        "spot_time": None,  # Timestamp of latest spot price update
        "futures_price": None,  # Latest futures price from aggTrade stream
        "futures_time": None,  # Timestamp of latest futures price update
        "funding_rate": None,  # Current funding rate from mark price stream
        # Previous prices for change detection (avoid redundant calculations)
        "last_spot_price": None,  # Previous spot price for comparison
        "last_futures_price": None,  # Previous futures price for comparison
        # Signal tracking state to prevent duplicate notifications
        "last_signal": None,  # Previous signal tuple (status, reason)
        "last_spread": None,  # Previous calculated spread value
        "last_funding": None,  # Previous funding rate (4 decimal precision)
        "last_all_conditions_met": False,  # Previous combined condition state
        # Position tracking for exit signal monitoring
        "in_position": False,  # Whether currently in an arbitrage position
    }

    async def handle_spot_trade(msg: dict) -> None:
        """Process incoming spot trade stream messages.

        Extracts price and timestamp from spot trade events and updates
        the shared state. Triggers signal evaluation after each update.

        Args:
            msg: WebSocket message dictionary containing trade data.
                Expected structure:
                {
                    "e": "trade",       // Event type
                    "E": 1672515782136, // Event time (epoch ms)
                    "s": "BNBBTC",      // Symbol
                    "t": 12345,         // Trade ID
                    "p": "0.001",       // Price
                    "q": "100",         // Quantity
                    "T": 1672515782136, // Trade time
                    "m": true,          // Is buyer market maker?
                    "M": true           // Ignore
                }

        Returns:
            None. Updates state and triggers evaluation asynchronously.

        Raises:
            KeyError: If required fields are missing from message.
            Exception: Any other errors are logged with full traceback.
        """
        try:
            # Handle both wrapped {"data": {...}} and unwrapped message formats
            # BinanceSocketManager may use different formats depending on stream type
            data = msg.get("data", msg)

            # Update state atomically to prevent partial updates
            async with state_lock:
                # Ignore error messages that contain 'e' field indicating an error
                if 'e' in data:
                    return
                
                # Extract and convert price to Decimal for precise arithmetic
                state["spot_price"] = Decimal(data["p"])
                # Store event timestamp for time-based analysis
                state["spot_time"] = int(data["E"])

            # Evaluate entry conditions with updated spot price
            await evaluate_signal()

        except KeyError as e:
            # Log missing field errors with symbol context for debugging
            logger.error(f"[{symbol}] Spot trade message missing key {e}: {msg}")
        except Exception as e:
            # Log unexpected errors with full stack trace
            logger.error(f"[{symbol}] Error handling spot trade: {e}", exc_info=True)

    async def handle_futures_aggtrade(msg: dict) -> None:
        """Process incoming futures aggregate trade stream messages.

        Extracts price and timestamp from futures aggTrade events and
        updates the shared state. Aggregate trades combine multiple
        individual trades for reduced message volume.

        Args:
            msg: WebSocket message dictionary containing aggregate trade data.
                Expected structure:
                {
                    "e": "aggTrade",  // Event type
                    "E": 123456789,   // Event time (epoch ms)
                    "s": "BTCUSDT",   // Symbol
                    "a": 5933014,     // Aggregate trade ID
                    "p": "0.001",     // Price
                    "q": "100",       // Total quantity
                    "nq": "100",      // Normal quantity (excluding RPI)
                    "f": 100,         // First trade ID
                    "l": 105,         // Last trade ID
                    "T": 123456785,   // Trade time
                    "m": true,        // Is buyer market maker?
                }

        Returns:
            None. Updates state and triggers evaluation asynchronously.

        Raises:
            KeyError: If required fields are missing from message.
            Exception: Any other errors are logged with full traceback.
        """
        try:
            # Handle both wrapped and unwrapped message formats
            data = msg.get("data", msg)

            # Update state atomically within lock
            async with state_lock:
                # Ignore error messages that contain 'e' field indicating an error
                if 'e' in data:
                    return
                
                # Store futures price as Decimal for precise spread calculation
                state["futures_price"] = Decimal(data["p"])
                state["futures_time"] = int(data["E"])

            # Re-evaluate entry conditions with new futures price
            await evaluate_signal()

        except KeyError as e:
            logger.error(f"[{symbol}] Futures aggregate trade message missing key {e}: {msg}")
        except Exception as e:
            logger.error(f"[{symbol}] Error handling futures aggregate trade: {e}", exc_info=True)

    async def handle_futures_mark_price(msg: dict) -> None:
        """Process incoming futures mark price stream messages.

        Extracts the funding rate from mark price updates. The mark price
        stream provides funding rate data every 3 seconds (fast=False) or
        every second (fast=True).

        Args:
            msg: WebSocket message dictionary containing mark price data.
                Expected structure:
                {
                    "e": "markPriceUpdate",  // Event type
                    "E": 1562305380000,      // Event time (epoch ms)
                    "s": "BTCUSDT",          // Symbol
                    "p": "11794.15000000",   // Mark price
                    "i": "11784.62659091",   // Index price
                    "P": "11784.25641265",   // Est. settle price
                    "r": "0.00038167",       // Funding rate
                    "T": 1562306400000       // Next funding time
                }

        Returns:
            None. Updates funding rate in shared state.

        Raises:
            KeyError: If required fields are missing from message.
            Exception: Any other errors are logged with full traceback.

        Note:
            This handler only extracts funding rate, not mark price.
            Futures price comes from the aggTrade stream for real-time accuracy.
        """
        try:
            # Extract data from potentially wrapped message
            data = msg.get("data", msg)

            # Update only funding rate from mark price stream
            # Mark price itself is not used; aggTrade provides real-time price
            async with state_lock:
                # Ignore error messages that contain 'e' field indicating an error
                if 'e' in data:
                    return
                
                state["funding_rate"] = Decimal(data["r"])

        except KeyError as e:
            logger.error(f"[{symbol}] Futures mark price message missing key {e}: {msg}")
        except Exception as e:
            logger.error(f"[{symbol}] Error handling futures mark price: {e}", exc_info=True)

    def to_humanize_time(epoch_ms: int) -> str:
        """Convert epoch milliseconds to human-readable timestamp string.

        Formats the timestamp in local timezone with millisecond precision
        for accurate logging and notification messages.

        Args:
            epoch_ms: Unix timestamp in milliseconds.

        Returns:
            str: Formatted timestamp string in "YYYY-MM-DD HH:MM:SS.mmm" format.

        Example:
            >>> to_humanize_time(1672515782136)
            '2023-01-01 00:03:02.136'
        """
        # Convert ms to seconds for strftime, then append milliseconds
        return (
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(epoch_ms / 1000))
            + f".{epoch_ms % 1000:03d}"
        )

    def calculate_spread(spot_price: Decimal, futures_price: Decimal) -> Decimal | None:
        """Calculate the percentage spread between spot and futures prices.

        The spread represents the premium/discount of futures relative to spot.
        A positive spread (futures > spot) indicates contango, which is required
        for the long-spot/short-futures arbitrage strategy.

        Args:
            spot_price: Current spot market price.
            futures_price: Current futures market price.

        Returns:
            Decimal | None: Spread as a decimal (e.g., 0.0028 for 0.28%),
                or None if either price is missing.

        Example:
            >>> calculate_spread(Decimal("50000"), Decimal("50140"))
            Decimal('0.0028')  # 0.28% spread
        """
        # Require both prices for valid spread calculation
        if spot_price and futures_price:
            return (futures_price - spot_price) / spot_price
        return None

    @ThrottledDebouncer(debounce_ms=100, max_wait_ms=500)
    async def evaluate_signal() -> None:
        """Evaluate entry and exit conditions and generate trading signals.

        This function is decorated with ThrottledDebouncer to prevent
        excessive evaluations during high-frequency updates.

        Entry conditions (when not in position):
        1. Positive spread (futures > spot)
        2. Spread exceeds entry threshold
        3. Funding rate is positive

        Exit condition (when in position):
        - Spread <= exit_max_spread threshold

        State transitions:
        - Not in position + entry conditions met -> Enter position, send ENTER signal
        - In position + exit condition met -> Exit position, send EXIT signal

        Args:
            None. Reads from shared state dictionary.

        Returns:
            None. Updates state and sends notifications as side effects.

        Raises:
            Exception: Any errors are logged; function does not propagate.

        Note:
            The ThrottledDebouncer decorator:
            - Debounces calls within 100ms window
            - Guarantees execution at least every 500ms during activity
        """
        try:
            async with state_lock:
                # Snapshot current state values for evaluation
                spot = state["spot_price"]
                spot_time = state["spot_time"]
                futures = state["futures_price"]
                futures_time = state["futures_time"]
                funding = state["funding_rate"]
                last_all_conditions_met = state["last_all_conditions_met"]
                last_spot_price = state["last_spot_price"]
                last_futures_price = state["last_futures_price"]
                in_position = state["in_position"]

                # Skip evaluation if data is incomplete or unchanged
                # This prevents redundant calculations and notifications
                if (
                    (spot == last_spot_price and futures == last_futures_price)
                    or spot is None
                    or futures is None
                    or funding is None
                ):
                    return

                # Calculate current spread percentage
                spread = calculate_spread(spot, futures)

                # Initialize notification variables
                should_notify = False
                notification_msg = None
                signal_status = None
                reason_text = ""

                if in_position:
                    # === EXIT SIGNAL EVALUATION ===
                    # Check if spread has fallen to exit threshold
                    exit_condition_met = spread <= exit_threshold

                    if exit_condition_met:
                        signal_status = "EXIT ⬆️"
                        should_notify = True
                        state["in_position"] = False
                        state["last_all_conditions_met"] = False

                else:
                    # === ENTRY SIGNAL EVALUATION ===
                    # Evaluate each entry condition independently
                    conditions = {
                        "positive_spread": futures > spot,  # Contango required
                        "spread_threshold": spread >= entry_threshold,  # Min profit margin
                        "funding_positive": funding > min_funding_rate,  # Positive carry
                    }

                    # Determine overall signal (all conditions must be true)
                    all_conditions_met = all(conditions.values())

                    # Build human-readable reason for WAIT signals
                    reasons = []
                    if not conditions["positive_spread"]:
                        reasons.append("Negative spread")
                    if not conditions["spread_threshold"] and conditions["positive_spread"]:
                        reasons.append("Spread too small")
                    if not conditions["funding_positive"]:
                        reasons.append("Negative funding")

                    reason_text = f" ({', '.join(reasons)})" if reasons else ""

                    # Determine if notification should be sent
                    # Notify on: entry signal, or any state transition
                    should_notify = all_conditions_met != last_all_conditions_met

                    if all_conditions_met:
                        signal_status = "ENTER ⬇️"
                        # Transition to in_position state
                        state["in_position"] = True
                    else:
                        signal_status = "WAIT ❌"

                    state["last_all_conditions_met"] = all_conditions_met

                # Log on: zero seconds on every hour
                localtime = time.localtime()
                should_log = localtime.tm_min == 0 and localtime.tm_sec == 0

                # Update state atomically before releasing lock
                state["last_spot_price"] = spot
                state["last_futures_price"] = futures
                state["last_funding"] = funding
                state["last_spread"] = spread
                state["last_signal"] = (signal_status, reason_text)

                # Prepare log message if logging is scheduled
                if should_log:
                    position_status = "IN_POSITION" if state["in_position"] else "NO_POSITION"
                    log_msg = (
                        f"{symbol} | "
                        f"{to_humanize_time(max(spot_time, futures_time))} | "
                        f"Spot: {spot} | Futures: {futures} | "
                        f"Spread: {spread * 100:.5f}% | Funding: {funding * 100:.5f}% | "
                        f"Status: {position_status}"
                    )

                # Prepare notification message while still holding state snapshot
                if should_notify and notifier:
                    notification_msg = (
                        f"{symbol} | "
                        f"{to_humanize_time(max(spot_time, futures_time))} | "
                        f"Spot: {spot} | Futures: {futures} | "
                        f"Spread: {spread * 100:.5f}% | Funding: {funding * 100:.5f}% | "
                        f"{signal_status}{reason_text}"
                    )

            # Perform I/O outside of lock to minimize lock contention
            if should_log:
                logger.info(log_msg)

            if should_notify and notifier:
                logger.info(notification_msg)
                await notifier.text(notification_msg)

        except Exception as e:
            logger.error(f"[{symbol}] Error in evaluate_signal: {e}", exc_info=True)

    async def process_stream(
        stream: ReconnectingWebsocket, handler: callable, symbol: str, stream_name: str
    ) -> None:
        """Process WebSocket stream messages in a continuous loop.

        Reads messages from the stream and dispatches them to the handler.
        Designed to drain the message queue as fast as possible to prevent
        backpressure and maintain real-time data.

        Args:
            stream: Active WebSocket stream connection.
            handler: Async function to process each message.
            symbol: Trading pair symbol for logging context.
            stream_name: Human-readable stream name for error messages.

        Returns:
            None. Runs indefinitely until stream error or cancellation.

        Raises:
            Exception: Stream connection errors are logged and re-raised
                to trigger reconnection logic.
        """
        try:
            # Continuous message processing loop
            while True:
                # Await next message from WebSocket stream
                msg = await stream.recv()
                try:
                    # Dispatch to handler; errors in handler don't break loop
                    await handler(msg)
                except Exception as e:
                    logger.error(f"[{symbol}] Error in {stream_name} handler: {e}")
        except Exception as e:
            # Log stream-level errors and propagate for reconnection
            logger.error(f"[{symbol}] {stream_name} stream error: {e}", exc_info=True)
            raise

    # WebSocket reconnection configuration
    max_retries: int | None = None  # None = retry indefinitely
    retry_count = 0  # Track consecutive failed attempts
    retry_delay = 3  # Initial delay between reconnection attempts (seconds)

    # Main reconnection loop - runs until max_retries or KeyboardInterrupt
    while max_retries is None or retry_count < max_retries:
        try:
            # Create WebSocket stream connections
            # Spot stream: individual trades for real-time spot price
            spot_stream = bsm.trade_socket(symbol)

            # Futures stream: aggregate trades for real-time futures price
            futures_stream = bsm.aggtrade_futures_socket(symbol)

            # Funding rate stream: mark price updates every 3 seconds
            funding_stream = bsm.symbol_mark_price_socket(symbol, fast=False)

            # Enter async context managers for all three streams
            async with (
                spot_stream as spot_ws,
                futures_stream as futures_ws,
                funding_stream as funding_ws,
            ):
                logger.info(
                    f"[{symbol}] WebSocket streams connected (attempt {retry_count + 1}). Monitoring for arbitrage signals..."
                )

                # Notify on successful reconnection after failures
                if retry_count > 0 and notifier:
                    await notifier.text(f"WebSocket reconnected after {retry_count} attempts")

                # Reset retry state on successful connection
                retry_count = 0
                retry_delay = 3

                # Create concurrent tasks for each stream processor
                spot_task = asyncio.create_task(
                    process_stream(spot_ws, handle_spot_trade, symbol, "Spot Aggregated Trade")
                )
                futures_task = asyncio.create_task(
                    process_stream(
                        futures_ws, handle_futures_aggtrade, symbol, "Futures Aggregated Trade"
                    )
                )
                funding_task = asyncio.create_task(
                    process_stream(
                        funding_ws, handle_futures_mark_price, symbol, "Futures Mark Price"
                    )
                )

                # Wait for all tasks (any exception will propagate)
                await asyncio.gather(spot_task, futures_task, funding_task)

        except KeyboardInterrupt:
            # Graceful shutdown on Ctrl+C
            logger.info(f"[{symbol}] Shutting down gracefully...")
            break

        except Exception as e:
            # Increment retry counter and log error
            retry_count += 1
            logger.error(
                f"[{symbol}] Error in main loop (attempt {retry_count}): {e}",
                exc_info=True,
            )

            # Notify about reconnection attempt
            if notifier:
                await notifier.text(
                    f"[{symbol}] WebSocket error detected. Reconnecting in {retry_delay}s... (attempt {retry_count})"
                )

            # Wait before retry attempt
            logger.info(f"[{symbol}] Reconnecting in {retry_delay} seconds...")
            await asyncio.sleep(retry_delay)

            # Exponential backoff: increase delay up to 30 seconds max
            retry_delay = min(retry_delay * 1.5, 30.0)

    logger.info(f"[{symbol}] Monitor terminated")


async def main(
    symbols: list[str],
    min_spread: float = 0.0024,
    safety_margin: float = 0.0004,
    min_funding_rate: float = 0.0,
    exit_max_spread: float = 0.0,
    notifier: Telegram | None = None,
) -> None:
    """Main entry point for the arbitrage spread monitor.

    Initializes the Binance client and socket manager, then starts
    concurrent monitoring tasks for all specified symbols.

    Args:
        symbols: List of trading pair symbols to monitor.
            Example: ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
        min_spread: Minimum spread percentage for entry signal.
            Default is 0.24% (0.0024). Represents minimum profit margin.
        safety_margin: Additional buffer added to min_spread.
            Default is 0.04% (0.0004). Accounts for execution slippage.
        min_funding_rate: Funding rate threshold for entry signal.
            Default is 0.0 (must be positive). Higher values = more selective.
        exit_max_spread: Maximum spread threshold to trigger exit signal.
            Default is 0.0 (0%). Exit signal fires when spread <= this value.
        notifier: Optional Telegram notifier for alerts.
            If None, signals are logged but not sent externally.

    Returns:
        None. Runs indefinitely until KeyboardInterrupt.

    Raises:
        KeyboardInterrupt: When user requests shutdown.
        Exception: Binance API errors are logged and may cause termination.

    Example:
        await main(
            symbols=["BTCUSDT"],
            min_spread=0.003,  # 0.3%
            safety_margin=0.0005,  # 0.05%
            min_funding_rate=0.0001,  # 0.01%
            exit_max_spread=0.0  # 0%
        )
    """
    # Calculate combined entry threshold for logging
    entry_threshold = Decimal(str(min_spread + safety_margin))

    # Log startup information
    logging.info("Starting Binance Spot-Futures Arbitrage Spread Monitor...")
    logging.info(f"Monitoring symbols: {', '.join(symbols)}")

    # Send startup notification if notifier is configured
    if notifier:
        await notifier.text(
            f"Starting Binance Spot-Futures Arbitrage Spread Monitor for {', '.join(symbols)}"
        )

    # Log configuration parameters for debugging
    logging.info(
        f"Configuration: MIN_SPREAD={min_spread * 100}%, SAFETY_MARGIN={safety_margin * 100}%, "
        f"ENTRY_THRESHOLD={entry_threshold * 100}%, EXIT_MAX_SPREAD={exit_max_spread * 100}%"
    )

    # Initialize Binance async client (no API keys needed for public data)
    client = await AsyncClient.create()

    # Calculate appropriate queue size based on number of symbols
    # Larger queue prevents message loss during high-volume periods
    queue_size = calculate_queue_size(symbols, buffer_seconds=10)

    # Create socket manager with calculated queue size
    bsm = BinanceSocketManager(client, max_queue_size=queue_size)
    logger.info(f"WebSocket max. queue size set to {queue_size}")

    try:
        # Create monitoring tasks for all symbols concurrently
        monitor_tasks = [
            asyncio.create_task(
                monitor_symbol(
                    symbol=symbol,
                    bsm=bsm,
                    notifier=notifier,
                    min_spread=min_spread,
                    safety_margin=safety_margin,
                    min_funding_rate=min_funding_rate,
                    exit_max_spread=exit_max_spread,
                )
            )
            for symbol in symbols
        ]

        # Run all monitors concurrently; exceptions don't cancel siblings
        await asyncio.gather(*monitor_tasks, return_exceptions=True)

    except KeyboardInterrupt:
        logging.info("Shutting down gracefully...")
    except Exception as e:
        logging.error(f"Error in main: {e}", exc_info=True)
    finally:
        # Cleanup: close Binance client connection
        await client.close_connection()

        # Send shutdown notification and close notifier
        if notifier:
            await notifier.text("Binance Spot-Futures Arbitrage Spread Monitor terminated")
            await notifier.close()


def calculate_queue_size(symbols: list[str], buffer_seconds: int = 10) -> int:
    """Calculate optimal WebSocket queue size based on symbol count.

    Estimates the number of messages expected per second across all
    streams and symbols, then applies a buffer to prevent queue overflow
    during traffic spikes.

    Args:
        symbols: List of trading symbols to monitor.
        buffer_seconds: Safety buffer in seconds. Larger values provide
            more headroom but use more memory. Default is 10 seconds.

    Returns:
        int: Recommended max_queue_size, rounded up to nearest 500.

    Example:
        >>> calculate_queue_size(["BTCUSDT", "ETHUSDT"], buffer_seconds=10)
        1000  # 2 symbols * 3 streams * 10 msg/s * 10s = 600, rounded to 1000
    """
    # Configuration constants for queue size calculation
    streams_per_symbol = 3  # spot, futures, funding streams
    avg_messages_per_second = 10  # Conservative estimate per stream

    # Calculate base queue size
    queue_size = len(symbols) * streams_per_symbol * avg_messages_per_second * buffer_seconds

    # Round up to nearest 500 for clean configuration
    return ((queue_size + 499) // 500) * 500


# Script entry point
if __name__ == "__main__":
    # Initialize logging system with console and rotating file handlers
    setup_logging()

    # Parse comma-separated symbol list from settings
    symbols = [s.strip() for s in settings.SYMBOLS.split(",") if s.strip()]

    # Initialize Telegram notifier if credentials are configured
    token = settings.TELEGRAM_BOT_TOKEN
    chat_id = settings.TELEGRAM_CHAT_ID

    # Only create notifier if both token and chat_id are provided
    notifier = Telegram(token=token, chat_id=chat_id) if token and chat_id else None
    logger.info("Telegram notifier %sconfigured" % ("NOT " if not notifier else ""))

    # Disable notifications in debug mode to avoid spam during development
    if settings.DEBUG:
        notifier = None

    # Load trading parameters from settings
    min_spread = settings.MIN_SPREAD
    safety_margin = settings.SAFETY_MARGIN
    min_funding_rate = settings.MIN_FUNDING_RATE
    exit_max_spread = settings.EXIT_MAX_SPREAD

    # Start the async event loop with main coroutine
    asyncio.run(
        main(symbols, min_spread, safety_margin, min_funding_rate, exit_max_spread, notifier)
    )
