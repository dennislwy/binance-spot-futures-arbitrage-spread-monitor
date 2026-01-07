# Binance Spot–Futures Arbitrage Basis Monitor

A real-time application that listens to **Binance Spot and USDT-M Futures WebSocket streams**, calculates the **spot–futures basis**, and applies a **funding-rate filter** to determine whether a **spot–futures arbitrage opportunity** is worth entering.

This project is intended as **Phase 1** of a spot–futures arbitrage system: **signal generation only** (no order execution).

---

## Features

- 📡 **Multi-symbol monitoring** - Track multiple trading pairs concurrently
- 🔄 Real-time **spot prices** via aggregated trade streams
- 📈 Real-time **futures mark prices** and **funding rates**
- 📊 Live **basis (%) calculation** for each symbol
- 🚦 Arbitrage entry signals based on:
  - Minimum basis threshold (fee-aware)
  - Safety margin
  - Funding-rate direction (short receives funding)
- 🔌 **WebSocket with auto-reconnection** - Exponential backoff retry logic
- ⚡ **Async programming** - High-performance concurrent stream processing
- 📝 **Rate-limited logging** - Only logs when signals or key metrics change
- 📱 **Telegram notifications** (optional) - Real-time alerts for signal changes
- 📅 **Daily rotating logs** - 7-day retention with automatic cleanup

---

## Arbitrage Logic

This script evaluates the classic arbitrage structure:

- **Long Spot BTC**
- **Short BTCUSDT Perpetual Futures**

An arbitrage signal is produced **only if all conditions are met**:

1. Futures price > Spot price (positive basis)
2. Basis ≥ minimum required basis (fees included)
3. Funding rate > 0 (short futures receives funding)
4. Safety margin is satisfied

---

## Basis Formula

```text
Basis (%) = (Futures_Price − Spot_Price) / Spot_Price × 100
```

## Entry Condition
```text
Basis ≥ (Minimum_Basis + Safety_Margin)
AND
Funding_Rate > 0
```

## Default Parameters
| Parameter         | Value |
| ----------------- | ----- |
| Spot maker fee    | 0.10% |
| Futures maker fee | 0.02% |
| Minimum basis     | 0.24% |
| Safety margin     | 0.04% |
| Entry threshold   | 0.28% |

## Installation

### Requirements
- Python 3.12+
- uv (Python package manager)
- Binance account (API key not required for public data streams)

### Install dependencies
```bash
# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Sync dependencies
uv sync
```

## Configuration

### Symbol Selection
Edit the `symbols` list in `main.py` to monitor different trading pairs:

```python
symbols = [
    "BTCUSDT",   # Bitcoin - Highest liquidity
    "ETHUSDT",   # Ethereum - Second major
    "BNBUSDT",   # Binance Coin - Native advantage
    "SOLUSDT",   # Solana - High volatility
    "XRPUSDT",   # Ripple - Volume leader
    "ADAUSDT",   # Cardano - Good liquidity
    "DOGEUSDT",  # Dogecoin - Meme coin volatility
    "POLUSDT",   # Polygon - DeFi token
]
```

**Recommended tiers:**
- **Tier 1** (BTC, ETH): Highest liquidity, tight spreads, stable funding
- **Tier 2** (BNB, SOL, XRP): High volume, good derivatives markets
- **Tier 3** (ADA, DOGE, POL): Moderate liquidity, higher volatility

### Arbitrage Parameters
Edit thresholds in `main.py` as needed:

```python
MIN_BASIS = 0.0024        # 0.24% - Minimum basis required
SAFETY_MARGIN = 0.0004    # 0.04% - Additional safety buffer
MIN_FUNDING_RATE = 0.0    # Funding must be positive
ENTRY_THRESHOLD = 0.28%   # Total threshold (MIN_BASIS + SAFETY_MARGIN)
```

### Telegram Notifications (Optional)
Create a `.env` file in the project root:

```env
TELEGRAM_BOT_TOKEN=your_bot_token_here
TELEGRAM_CHAT_ID=your_chat_id_here
```

To set up Telegram notifications:
1. Create a bot via [@BotFather](https://t.me/botfather)
2. Get your chat ID via [@userinfobot](https://t.me/userinfobot)
3. Add credentials to `.env` file

## Usage

### Run the Monitor

#### Option 1: Direct Execution
```bash
python main.py
```

#### Option 2: Run as Systemd Service (Linux)

For production deployment, you can run the monitor as a systemd service that starts automatically on boot and restarts on failure.

**Register the service:**
```bash
./service-register.sh
```

This creates and enables a systemd service with:
- Automatic restart on failure (10-second delay)
- Network dependency (waits for network connectivity)
- Journal logging for easy log access
- Runs as your current user

**Manage the service:**
```bash
# Start the service
./service-start.sh
# or: sudo systemctl start spot-futures-arbitrage-basis-monitor

# Stop the service
./service-stop.sh
# or: sudo systemctl stop spot-futures-arbitrage-basis-monitor

# Restart the service
./service-restart.sh
# or: sudo systemctl restart spot-futures-arbitrage-basis-monitor

# Check service status
./service-status.sh
# or: sudo systemctl status spot-futures-arbitrage-basis-monitor

# View live logs
sudo journalctl -u spot-futures-arbitrage-basis-monitor -f

# View logs from last boot
sudo journalctl -u spot-futures-arbitrage-basis-monitor -b
```

**Unregister the service:**
```bash
sudo systemctl stop spot-futures-arbitrage-basis-monitor
sudo systemctl disable spot-futures-arbitrage-basis-monitor
sudo rm /etc/systemd/system/spot-futures-arbitrage-basis-monitor.service
sudo systemctl daemon-reload
```

### Example Output
```text
2026-01-07 22:53:28 [   __main__][evaluate_signal_loop][ INFO] BTCUSDT | Spot: 91,894.5000 | Futures: 91,850.6100 | Basis: -0.048% | Funding: 0.0037% | WAIT ❌ (Negative basis)
2026-01-07 22:53:28 [   __main__][evaluate_signal_loop][ INFO] ETHUSDT | Spot: 3,191.9000 | Futures: 3,189.5100 | Basis: -0.075% | Funding: 0.0022% | WAIT ❌ (Negative basis)
2026-01-07 22:53:29 [   __main__][evaluate_signal_loop][ INFO] BTCUSDT | Spot: 91,899.2400 | Futures: 91,853.9000 | Basis: -0.049% | Funding: 0.0037% | WAIT ❌ (Negative basis)
2026-01-07 22:53:30 [   __main__][evaluate_signal_loop][ INFO] ETHUSDT | Spot: 3,191.4100 | Futures: 3,190.3400 | Basis: -0.034% | Funding: 0.0022% | WAIT ❌ (Negative basis)
```

### Signal Interpretation
- **ENTER ✅** - All conditions met, arbitrage opportunity detected
- **WAIT ❌** - Conditions not met, with reason(s):
  - `Negative basis` - Spot price higher than futures
  - `Basis too small` - Basis below entry threshold
  - `Funding unfavorable` - Negative funding rate

### Logs
Application logs are stored in:
- **Location**: `log/app.log`
- **Rotation**: Daily at midnight
- **Retention**: 7 days of backups
- **Format**: Timestamped with function name and log level

## Architecture

### Multi-Symbol Concurrent Monitoring
The application uses Python's `asyncio` to monitor multiple trading pairs concurrently:

```
┌─────────────────────────────────────────┐
│          Main Application               │
│  (AsyncClient + BinanceSocketManager)   │
└─────────────────┬───────────────────────┘
                  │
        ┌─────────┴─────────┐
        │                   │
   ┌────▼─────┐      ┌─────▼────┐
   │ BTCUSDT  │      │ ETHUSDT  │  ... (N symbols)
   │ Monitor  │      │ Monitor  │
   └────┬─────┘      └─────┬────┘
        │                   │
   ┌────┴──────┐      ┌────┴──────┐
   │ Spot WS   │      │ Spot WS   │
   │ Futures WS│      │ Futures WS│
   │ Eval Loop │      │ Eval Loop │
   └───────────┘      └───────────┘
```

**Key Components:**

1. **`monitor_symbol()`** - Independent monitor for each symbol
   - Manages WebSocket connections (spot + futures)
   - Maintains separate state for prices, funding, and signals
   - Auto-reconnects with exponential backoff on failures
   - Evaluates arbitrage signals every 1 second

2. **`main()`** - Orchestrates all monitors
   - Creates concurrent tasks for all symbols
   - Shares single `BinanceSocketManager` instance
   - Handles graceful shutdown and cleanup

3. **`process_stream()`** - High-speed message processor
   - Drains WebSocket queues as fast as possible
   - Non-blocking message handling
   - Error isolation per stream

### WebSocket Reliability Features

- **Large queue size (2000)** - Prevents message loss during bursts
- **Automatic reconnection** - Retries indefinitely with exponential backoff (3-60s)
- **Connection health monitoring** - Detects and recovers from failures
- **Per-symbol isolation** - One symbol's failure doesn't affect others

### Performance Optimizations

- **Aggregated trade streams** - Reduced message volume (vs raw trades)
- **Rate-limited logging** - Only logs on signal/metric changes
- **Async I/O throughout** - Non-blocking concurrent operations
- **Minimal state updates** - Fast in-memory operations only

## Project Structure

```
spot-futures-arbitrage-basis-monitor/
├── main.py                 # Main application entry point
├── config.py              # Configuration settings loader
├── notifications.py       # Telegram notification handler
├── pyproject.toml         # Project dependencies (uv)
├── uv.lock               # Locked dependencies
├── .env                  # Environment variables (not in repo)
├── log/                  # Log files (auto-created)
│   └── app.log          # Main application log (rotates daily)
├── service-register.sh   # Register as systemd service
├── service-start.sh      # Start the systemd service
├── service-stop.sh       # Stop the systemd service
├── service-restart.sh    # Restart the systemd service
├── service-status.sh     # Check service status
├── CLAUDE.md            # Development documentation
└── README.md            # This file
```

## Troubleshooting

### WebSocket Connection Issues

**Symptom**: "WebSocket error detected. Reconnecting..."

**Solutions:**
1. Check your internet connection
2. Verify Binance API is accessible (not blocked by firewall)
3. Application will auto-retry with exponential backoff
4. Check logs in `log/app.log` for detailed error messages

### No Data Flowing

**Symptom**: Application starts but no price updates appear

**Solutions:**
1. Verify symbols exist on Binance Spot and USDT-M Futures
2. Check if symbol has active futures contract
3. Ensure symbol format is correct (e.g., "BTCUSDT" not "BTC/USDT")
4. Wait 1-2 seconds for initial data population

### High CPU Usage

**Symptom**: Excessive CPU consumption

**Solutions:**
1. Reduce number of monitored symbols
2. Increase evaluation interval (currently 1 second)
3. Check for infinite reconnection loops in logs
4. Ensure queue size is adequate for message volume

### Missing Log Files

**Symptom**: No logs in `log/` directory

**Solutions:**
1. Check write permissions for `log/` directory
2. Application auto-creates directory on startup
3. Verify logging is configured in `main.py`

### Systemd Service Issues

**Symptom**: Service fails to start or keeps restarting

**Solutions:**
1. Check service status: `sudo systemctl status spot-futures-arbitrage-basis-monitor`
2. View detailed logs: `sudo journalctl -u spot-futures-arbitrage-basis-monitor -n 100`
3. Verify uv is installed at `~/.local/bin/uv`
4. Check `.env` file exists and has correct permissions
5. Ensure working directory path is correct in service file
6. Verify Python dependencies are installed: `uv sync`

**Symptom**: Service logs not appearing in journal

**Solutions:**
1. Check StandardOutput/StandardError are set to `journal` in service file
2. Logs also written to `log/app.log` as backup
3. Use `sudo journalctl -u spot-futures-arbitrage-basis-monitor --no-pager` to see all logs

## Limitations & Future Enhancements

**Current Limitations:**
- Signal generation only (no order execution)
- Requires manual symbol configuration
- Single configuration for all symbols (same thresholds)
- No historical data analysis or backtesting

**Planned Enhancements (Phase 2+):**
- Automated order execution
- Per-symbol threshold configuration
- Historical basis analysis and statistics
- Position tracking and P&L calculation
- Risk management modules
- Web dashboard for monitoring
- Database storage for signal history

## Contributing

This is a personal project for spot-futures arbitrage signal monitoring. Feel free to fork and adapt for your own use.

## License

MIT License - See LICENSE file for details

## Disclaimer

**This software is for educational and informational purposes only.**

- Not financial advice
- No warranty or guarantees provided
- Use at your own risk
- Cryptocurrency trading involves substantial risk
- Past performance does not indicate future results
- Always test with small amounts first

**Trading cryptocurrencies carries risk of financial loss. The authors are not responsible for any losses incurred through use of this software.**
