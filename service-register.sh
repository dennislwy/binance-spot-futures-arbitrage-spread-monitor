#!/bin/bash

SERVICE_NAME="spot-futures-arbitrage-basis-monitor"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Registering $SERVICE_NAME as a systemd service..."

# Create systemd service unit file
sudo tee $SERVICE_FILE > /dev/null <<EOF
[Unit]
Description=Spot-Futures Arbitrage Basis Monitor
After=network-online.target
Wants=network-online.target

[Service]
User=$(whoami)
Type=simple
WorkingDirectory=$APP_DIR
ExecStart=$HOME/.local/bin/uv run python main.py
Restart=on-failure
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd daemon and enable service
sudo systemctl daemon-reload
sudo systemctl enable $SERVICE_NAME

echo "✓ Service registered successfully!"
echo "Start the service with: sudo systemctl start $SERVICE_NAME"
echo "Check status with: sudo systemctl status $SERVICE_NAME"
echo "View logs with: sudo journalctl -u $SERVICE_NAME -f"