#!/bin/bash
# AUM Automation Tool launcher

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Check Python
if ! command -v python3 &>/dev/null; then
    echo "Error: python3 not found. Install Python 3.9+."
    exit 1
fi

# Check Azure CLI
if ! command -v az &>/dev/null; then
    echo "Error: Azure CLI not found."
    echo "Install from: https://docs.microsoft.com/cli/azure/install-azure-cli"
    exit 1
fi

# Activate venv if present
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Create required directories
mkdir -p logs reports state

# Check config
if [ ! -f "config/config.yaml" ]; then
    echo "No config/config.yaml found."
    echo "Copy config/config.example.yaml to config/config.yaml and fill in your subscription IDs."
    exit 1
fi

python3 main_gui.py "$@"
