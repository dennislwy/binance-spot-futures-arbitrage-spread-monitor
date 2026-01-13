import asyncio
import logging
import sys
import time
from decimal import Decimal
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from binance import AsyncClient, BinanceSocketManager

from core.config import settings
from core.throttled_debouncer import ThrottledDebouncer
from notifications import Telegram

logger = logging.getLogger(__name__)

async def monitor_symbol(
    symbol: str,
    bsm: BinanceSocketManager,
    notifier: Telegram | None = None,
    min_spread: float = 0.0024,
    safety_margin: float = 0.0004,
    min_funding_rate: float = 0.0,
):
    entry_threshold = Decimal(str(min_spread + safety_margin))

    # Shared state for latest prices and funding rate (per symbol)
    state = {
        "spot_price": None,  # Latest spot price
        "spot_time": None,  # Timestamp of latest spot price
        "futures_price": None,  # Latest futures price
        "futures_time": None,  # Timestamp of latest futures price
        "funding_rate": None,  # Latest funding rate
        
        "last_spot_price": None,  # Track spot price changes (recalculate spread if needed)
        "last_futures_price": None,  # Track futures price changes (recalculate spread if needed)
        
        "last_signal": None,  # Track last signal to avoid duplicate logs
        "last_spread": None,  # Track last spread
        "last_funding": None,  # Track last funding rate (4 decimal accuracy)
        "last_all_conditions_met": False,  # Track if all conditions were met
    }

    async def handle_spot_trade(msg: dict):
        """
        Handle spot trade stream updates
        
        {
            "e": "trade",       // Event type
            "E": 1672515782136, // Event time
            "s": "BNBBTC",      // Symbol
            "t": 12345,         // Trade ID
            "p": "0.001",       // Price
            "q": "100",         // Quantity
            "T": 1672515782136, // Trade time
            "m": true,          // Is the buyer the market maker?
            "M": true           // Ignore
        }        
        """
        try:
            # Extract data from nested structure
            data = msg.get("data", msg)
            
            # Get spot price from trade stream (just update state, don't log)
            state["spot_price"] = Decimal(data["p"])
            state["spot_time"] = int(data["E"])
            # logger.info(f"[{symbol}] Spot trade update: price={state['spot_price']}, time={state['spot_time']}, trade id={data['t']}")
            
            await evaluate_signal()
            
        except KeyError as e:
            logger.error(f"Spot trade message missing key {e}: {msg}")
        except Exception as e:
            logger.error(f"Error handling spot trade: {e}", exc_info=True)

    async def handle_futures_aggtrade(msg: dict):
        """
        Handle futures aggregate trade stream updates
        
        {
            "e": "aggTrade",  // Event type
            "E": 123456789,   // Event time
            "s": "BTCUSDT",   // Symbol
            "a": 5933014,     // Aggregate trade ID
            "p": "0.001",     // Price
            "q": "100",       // Quantity with all the market trades
            "nq": "100",      // Normal quantity without the trades involving RPI orders
            "f": 100,         // First trade ID
            "l": 105,         // Last trade ID
            "T": 123456785,   // Trade time
            "m": true,        // Is the buyer the market maker?
        }
        """
        try:
            # Extract data from nested structure
            data = msg.get("data", msg)
            
            # Get futures aggregate trade
            state["futures_price"] = Decimal(data["p"])
            state["futures_time"] = int(data["E"])
            # logger.info(f"[{symbol}] Futures aggregate trade update: price={state['futures_price']}, time={state['futures_time']}, trade id={data['a']}")
            
            await evaluate_signal()
            
        except KeyError as e:
            logger.error(f"Futures aggregate trade message missing key {e}: {msg}")
        except Exception as e:
            logger.error(f"Error handling futures aggregate trade: {e}", exc_info=True)
            
    async def handle_futures_mark_price(msg: dict):
        """
        Handle futures mark price stream updates
        
        {
            "e": "markPriceUpdate",  	// Event type
            "E": 1562305380000,      	// Event time
            "s": "BTCUSDT",          	// Symbol
            "p": "11794.15000000",   	// Mark price
            "i": "11784.62659091",		// Index price
            "P": "11784.25641265",		// Estimated Settle Price, only useful in the last hour before the settlement starts
            "r": "0.00038167",       	// Funding rate
            "T": 1562306400000       	// Next funding time
        }
        """
        try:
            # Extract data from nested structure
            data = msg.get("data", msg)
            
            # # Get futures mark price
            # state["futures_price"] = Decimal(data["p"])
            # state["futures_time"] = int(data["E"])
            # logger.debug(f"Futures mark price update: price={state['futures_price']}, time={state['futures_time']}")
            
            # Get funding rate from mark price stream
            state["funding_rate"] = Decimal(data["r"])
            # logger.info(f"[{symbol}] Futures mark price update: price={data['p']}, funding_rate={state['funding_rate']*100}%, time={data['E']}")
            
        except KeyError as e:
            logger.error(f"Futures mark price message missing key {e}: {msg}")
        except Exception as e:
            logger.error(f"Error handling futures mark price: {e}", exc_info=True)

    def to_humanize_time(epoch_ms: int) -> str:
        """Convert epoch milliseconds to human-readable time string (up to milliseconds)"""
        return time.strftime(
            "%Y-%m-%d %H:%M:%S", time.localtime(epoch_ms / 1000)
        ) + f".{epoch_ms % 1000:03d}"

    @ThrottledDebouncer(debounce_ms=100, max_wait_ms=500)
    async def evaluate_signal():
        try:
            spot = state["spot_price"]
            spot_time = state["spot_time"]
            futures = state["futures_price"]
            futures_time = state["futures_time"]
            funding = state["funding_rate"]
            
            # Wait until we have all data and prices have changed
            if ((spot == state["last_spot_price"] and futures == state["last_futures_price"]) 
                or spot is None 
                or futures is None 
                or funding is None):
                # logger.warning(f"[{symbol}] Skipping evaluation: incomplete data or no price change (spot: {spot}, futures: {futures}, funding: {funding})")
                return
            
            # Calculate new spread
            spread = calculate_spread(spot, futures)
            
            # Evaluate conditions
            conditions = {
                "positive_spread": futures > spot,
                "spread_threshold": spread >= entry_threshold,
                "funding_positive": funding > min_funding_rate,
            }
            
            # Determine signal
            all_conditions_met = all(conditions.values())
            
            # Format output
            signal_status = "ENTER ✅" if all_conditions_met else "WAIT ❌"

            # Build reason if not entering
            reasons = []
            if not conditions["positive_spread"]:
                reasons.append("Negative spread")
            if not conditions["spread_threshold"] and conditions["positive_spread"]:
                reasons.append("Spread too small")
            if not conditions["funding_positive"]:
                reasons.append("Negative funding")

            reason_text = f" ({', '.join(reasons)})" if reasons else ""

            # Create current signal snapshot
            current_signal = (signal_status, reason_text)
            
            # Rate limiting: Only log if signal changed OR spread changed OR funding changed
            # should_log = (
            #     current_signal != state["last_signal"]
            #     or spread != state["last_spread"]
            #     or funding != state["last_funding"]
            # )

            # if should_log:
            #     # Log the signal
            #     logger.info(
            #         f"{symbol} | "
            #         f"Spot: {spot} | Futures: {futures} | "
            #         f"Spread: {spread*100:.4f}% | Funding: {funding*100}% | "
            #         f"{signal_status}{reason_text}"
            #     )
            
            if notifier:
                should_notify = all_conditions_met or all_conditions_met != state["last_all_conditions_met"]
                
                if should_notify:
                    msg = (f"{symbol} | "
                        f"{to_humanize_time(max(spot_time, futures_time))} | "
                        f"Spot: {spot} | Futures: {futures} | "
                        f"Spread: {spread*100:.4f}% | Funding: {funding*100}% | "
                        f"{signal_status}{reason_text}")
                    
                    logger.info(msg)
                    await notifier.text(msg)

            # Update last known states
            state["last_signal"] = current_signal
            state["last_spread"] = spread
            state["last_funding"] = funding
            state["last_all_conditions_met"] = all_conditions_met
            state["last_spot_price"] = spot
            state["last_futures_price"] = futures
                    
        except Exception as e:
            logger.error(f"Error in evaluate_signal: {e}", exc_info=True)

    # Start WebSocket streams with automatic reconnection
    max_retries: int | None = None  # Retry indefinitely
    retry_count = 0
    retry_delay = 3  # seconds between retries

    while max_retries is None or retry_count < max_retries:
        try:
            # Spot stream
            # spot_stream = bsm.aggtrade_socket(symbol)  # real-time (lower traffic)
            spot_stream = bsm.trade_socket(symbol)  # real-time (higher traffic)

            # Futures stream
            futures_stream = bsm.aggtrade_futures_socket(symbol)  # 100ms updates

            # Funding rate stream via mark price updates
            funding_stream = bsm.symbol_mark_price_socket(symbol, fast=False)  # 3s updates

            # Create tasks for both streams
            async with spot_stream as spot_ws, futures_stream as futures_ws, funding_stream as funding_ws:
                logger.info(
                    f"WebSocket streams connected (attempt {retry_count + 1}). Monitoring {symbol} for arbitrage signals..."
                )
                if retry_count > 0 and notifier:
                    await notifier.text(f"WebSocket reconnected after {retry_count} attempts")

                # Reset retry counter on successful connection
                retry_count = 0
                retry_delay = 3  # reset delay

                # Process messages concurrently
                spot_task = asyncio.create_task(
                    process_stream(spot_ws, handle_spot_trade, "Spot Aggregated Trade")
                )
                futures_task = asyncio.create_task(
                    process_stream(futures_ws, handle_futures_aggtrade, "Futures Aggregated Trade")
                )
                funding_task = asyncio.create_task(
                    process_stream(funding_ws, handle_futures_mark_price, "Futures Mark Price")
                )

                # Wait for all tasks
                await asyncio.gather(spot_task, futures_task, funding_task)

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
            retry_delay = min(retry_delay * 1.5, 60.0)

    logger.info(f"Monitor for {symbol} terminated")

async def main(
    symbols: list[str], 
    min_spread: float = 0.0024,
    safety_margin: float = 0.0004,
    min_funding_rate: float = 0.0,
    notifier: Telegram | None = None):
    """Main entry point for monitoring multiple symbols concurrently.

    Args:
        symbols: List of trading pair symbols to monitor (e.g., ["BTCUSDT", "ETHUSDT"])
        notifier: Optional Telegram notifier
    """
    entry_threshold = Decimal(str(min_spread + safety_margin))  # 0.28%

    logging.info("Starting Binance Spot-Futures Arbitrage Spread Monitor...")
    logging.info(f"Monitoring symbols: {', '.join(symbols)}")
    if notifier:
        await notifier.text(
            f"Starting Binance Spot-Futures Arbitrage Spread Monitor for {', '.join(symbols)}"
        )

    logging.info(
        f"Configuration: MIN_SPREAD={min_spread*100}%, SAFETY_MARGIN={safety_margin*100}%, ENTRY_THRESHOLD={entry_threshold*100}%"
    )

    client = await AsyncClient.create()
    # Increase queue size to handle high-frequency market data streams
    # For multiple symbols, we need even more buffer capacity
    # Using 2000 to handle multiple concurrent streams
    bsm = BinanceSocketManager(client, max_queue_size=2000)

    try:
        # Create monitor tasks for all symbols
        monitor_tasks = [
            asyncio.create_task(
                monitor_symbol(
                    symbol=symbol,
                    bsm=bsm,
                    notifier=notifier,
                    min_spread=min_spread,
                    safety_margin=safety_margin,
                    min_funding_rate=min_funding_rate,
                )
            )
            for symbol in symbols
        ]

        # Run all monitors concurrently
        await asyncio.gather(*monitor_tasks, return_exceptions=True)

    except KeyboardInterrupt:
        logging.info("Shutting down gracefully...")
    except Exception as e:
        logging.error(f"Error in main: {e}", exc_info=True)
    finally:
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
    
def calculate_spread(spot_price: Decimal, futures_price: Decimal) -> Decimal | None:
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

    # List of symbols to monitor
    symbols = [s.strip() for s in settings.SYMBOLS.split(",") if s.strip()]

    # Initialize Telegram notifier
    token = settings.TELEGRAM_BOT_TOKEN
    chat_id = settings.TELEGRAM_CHAT_ID
    notifier = Telegram(token=token, chat_id=chat_id) if token and chat_id else None
    logger.info("Telegram notifier %sconfigured" % ("NOT " if not notifier else ""))
    if settings.DEBUG:
       notifier = None 
    
    min_spread = settings.MIN_SPREAD
    safety_margin = settings.SAFETY_MARGIN
    min_funding_rate = settings.MIN_FUNDING_RATE

    asyncio.run(main(symbols, min_spread, safety_margin, min_funding_rate, notifier))
