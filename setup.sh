#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

echo "🚀 Starting environment setup..."

# 1. Create Python Virtual Environment if it doesn't exist
if [ ! -d ".venv" ]; then
    echo "Creating Python Virtual Environment (.venv)..."
    python3 -m venv .venv
else
    echo "✅ .venv already exists."
fi

# 2. Activate and install python requirements
echo "Installing Python dependencies..."
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
pip install -r requirements-dev.txt

# 3. Create local directories for logs and checkpoints
echo "Creating necessary local data directories..."
mkdir -p data/checkpoints data/logs

# 4. Create .env from template if it doesn't exist
if [ ! -f ".env" ]; then
    echo "Creating .env from template..."
    if [ -f ".env.example" ]; then
        cp .env.example .env
        echo "⚠️  Please edit your .env file with your secrets!"
    else
        touch .env
        echo "✅ Created empty .env file."
    fi
fi

echo "🎉 Setup complete! You are ready to run the project."