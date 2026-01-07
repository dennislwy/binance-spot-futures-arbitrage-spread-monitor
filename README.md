# Binance Spot–Futures ArbitrageBasis Monitor

A real-time application that listens to **Binance Spot and USDT-M Futures WebSocket streams**, calculates the **spot–futures basis**, and applies a **funding-rate filter** to determine whether a **spot–futures arbitrage opportunity** is worth entering.

This project is intended as **Phase 1** of a spot–futures arbitrage system: **signal generation only** (no order execution).

---

## Features

- 📡 Real-time **BTCUSDT spot price** (trade stream)
- 📡 Real-time **BTCUSDT futures mark price**
- 💰 Real-time **funding rate monitoring**
- 📊 Live **basis (%) calculation**
- 🚦 Arbitrage entry signal based on:
  - Minimum basis threshold (fee-aware)
  - Safety margin
  - Funding-rate direction (short receives funding)
- Websocket
- async programming

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
- Binance account (API key not required for public data)

### Install dependencies
```bash
uv add python-binance asyncio
```

## Usage
Run the script:
```bash
python main.py
```

Example output:
```text
BTCUSDT | Spot: 40000.00 | Futures: 40120.00 | Basis: 0.300% | Funding: 0.0100% | ENTER ✅
BTCUSDT | Spot: 40010.20 | Futures: 40080.40 | Basis: 0.175% | Funding: 0.0100% | WAIT ❌ (Basis too small)
BTCUSDT | Spot: 40000.50 | Futures: 40130.10 | Basis: 0.324% | Funding: -0.0100% | WAIT ❌ (Funding unfavorable)
```

## Configuration
Edit parameters in `main.py` as needed:
```python
MIN_BASIS = 0.0024        # 0.24%
SAFETY_MARGIN = 0.0004    # 0.04%
MIN_FUNDING_RATE = 0.0   # funding must be positive
```
