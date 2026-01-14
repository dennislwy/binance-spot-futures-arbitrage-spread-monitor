#!/bin/bash

SERVICE_NAME="binance-spot-futures-arbitrage-spread-monitor"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

function register() {
    echo "Registering $SERVICE_NAME as a systemd service..."
    
    sudo tee $SERVICE_FILE > /dev/null <<EOF
[Unit]
Description=Spot-Futures Arbitrage Spread Monitor
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

    sudo systemctl daemon-reload
    sudo systemctl enable $SERVICE_NAME
    
    echo "✓ Service registered successfully!"
    echo "Start the service with: . service.sh start"
    echo "Check status with: . service.sh status"
}

function unregister() {
    echo "Unregistering $SERVICE_NAME service..."
    sudo systemctl stop $SERVICE_NAME 2>/dev/null
    sudo systemctl disable $SERVICE_NAME 2>/dev/null
    sudo rm -f $SERVICE_FILE
    sudo systemctl daemon-reload
    echo "✓ Service unregistered successfully!"
}

function start() {
    echo "Starting $SERVICE_NAME service..."
    sudo systemctl start $SERVICE_NAME
    echo "✓ Service started!"
}

function stop() {
    echo "Stopping $SERVICE_NAME service..."
    sudo systemctl stop $SERVICE_NAME
    echo "✓ Service stopped!"
}

function restart() {
    echo "Restarting $SERVICE_NAME service..."
    sudo systemctl restart $SERVICE_NAME
    echo "✓ Service restarted!"
}

function status() {
    sudo systemctl status $SERVICE_NAME
}

function show_usage() {
    echo "Usage: . service.sh <command>"
    echo ""
    echo "Commands:"
    echo "  register    - Register the application as a systemd service"
    echo "  unregister  - Unregister and remove the systemd service"
    echo "  start       - Start the service"
    echo "  stop        - Stop the service"
    echo "  restart     - Restart the service"
    echo "  status      - Show service status"
    echo ""
    echo "Example: . service.sh register"
}

# Main script logic
case "$1" in
    register)
        register
        ;;
    unregister)
        unregister
        ;;
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        restart
        ;;
    status)
        status
        ;;
    *)
        show_usage
        ;;
esac
