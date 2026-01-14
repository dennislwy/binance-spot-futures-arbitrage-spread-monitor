# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a real-time Python application that monitors Binance spot and USDT-M futures markets to detect spot-futures arbitrage opportunities. The project is **Phase 1** of a spot-futures arbitrage system focused solely on **signal generation** (no automated trading).

**Core Strategy:**
- Long Spot BTC + Short BTCUSDT Perpetual Futures
- Entry signal requires: positive spread, minimum threshold met, positive funding rate, and safety margin

## Running the Application

### Install Dependencies
```bash
uv sync
```

### Run the Monitor
```bash
python main.py
```

The application will start logging to both console and `log/app.log` with daily rotation (7 days retention).

## Project Architecture

### Entry Point: `main.py`
- `setup_logging()`: Configures dual logging (console + rotating file handler in `log/`)
- `main()`: Async entry point that initializes Binance client and socket manager
- `calculate_spread(spot_price, futures_price)`: Calculates spot-futures spread percentage

### Key Components (To Be Implemented)
The main application loop is currently a placeholder. Implementation should:
1. Subscribe to Binance WebSocket streams for:
   - Spot trade stream (BTCUSDT spot price)
   - Futures mark price stream (BTCUSDT perpetual)
   - Funding rate stream
2. Calculate real-time spread: `(futures_price - spot_price) / spot_price`
3. Evaluate entry conditions based on configured thresholds

### Configuration Parameters (in `main.py`)
- `MIN_SPREAD`: Minimum spread threshold (default: 0.24%)
- `SAFETY_MARGIN`: Additional safety buffer (default: 0.04%)
- `MIN_FUNDING_RATE`: Funding rate threshold (default: 0.0, must be positive)

Fee structure assumed:
- Spot maker fee: 0.10%
- Futures maker fee: 0.02%
- Entry threshold: 0.28% (MIN_SPREAD + SAFETY_MARGIN)

## Technology Stack

- **Python 3.12+** (managed via `uv`)
- **python-binance**: Binance API client for async WebSocket connections
- **asyncio**: Async/await pattern for concurrent stream processing

## Logging System

The application uses Python's `logging` module with:
- **Console handler**: Real-time output to stdout
- **File handler**: `TimedRotatingFileHandler` that rotates daily at midnight
- **Log location**: `log/app.log` (creates directory automatically)
- **Retention**: 7 days of backup logs with date suffixes (`%Y-%m-%d`)
- **Format**: `%(asctime)s [%(name)20.20s][%(funcName)20.20s][%(levelname)5.5s] %(message)s`

## Development Notes

### Async Programming
This project uses Python's asyncio exclusively. When implementing:
- Use `async def` for all Binance WebSocket handlers
- Use `await` for all I/O operations (WebSocket messages, API calls)
- The `BinanceSocketManager` from python-binance provides async context managers for streams

### No API Keys Required
This application only consumes public market data streams. No authentication or API keys are needed.

### WebSocket Streams Architecture
Use `BinanceSocketManager` to create multiple concurrent streams:
- Each stream runs in its own async task
- Share state between streams using async-safe data structures
- Handle reconnection and error scenarios gracefully

## Notes for Claude Code
- follow async programming patterns throughout the codebase
- use Context7 MCP toolset to check up-to-date docs when needed for implementing new libraries or frameworks, or adding features using them
- use Google Style Python Docstrings
- use mermaid markdown (without any style or fill color directives) for visual diagrams
