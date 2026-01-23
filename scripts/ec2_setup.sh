#!/bin/bash
# EC2 Instance Setup Script
# Run this on a fresh Ubuntu 22.04+ EC2 instance to set up the worker
#
# Usage: curl -sSL <raw-url> | bash
# Or: ssh ubuntu@<ip> 'bash -s' < scripts/ec2_setup.sh

set -e

echo "=== Sadie GTM EC2 Worker Setup ==="

# Update system
echo "Updating system packages..."
sudo apt-get update -qq
sudo apt-get upgrade -y -qq

# Install dependencies
echo "Installing dependencies..."
sudo apt-get install -y -qq git curl unzip

# Install uv (Python package manager)
echo "Installing uv..."
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"

# Clone repo (if not exists)
if [ ! -d ~/sadie-gtm ]; then
    echo "Cloning repository..."
    git clone https://github.com/aliabbassi-1337/sadie-gtm.git ~/sadie-gtm
fi

cd ~/sadie-gtm

# Install Python dependencies
echo "Installing Python dependencies..."
uv sync

# Install Playwright browsers
echo "Installing Playwright browsers..."
uv run playwright install chromium
uv run playwright install-deps chromium

# Install playwright-stealth
echo "Installing playwright-stealth..."
uv pip install playwright-stealth

# Create .env file if not exists
if [ ! -f .env ]; then
    echo "Creating .env file (you need to fill in the values)..."
    cat > .env << 'EOF'
# Database
SADIE_DB_HOST=aws-1-ap-southeast-1.pooler.supabase.com
SADIE_DB_PORT=6543
SADIE_DB_NAME=postgres
SADIE_DB_USER=postgres.yunairadgmaqesxejqap
SADIE_DB_PASSWORD=SadieGTM321-

# AWS
SQS_DETECTION_QUEUE_URL=https://sqs.eu-north-1.amazonaws.com/760711518969/detection-queue
AWS_REGION=eu-north-1
EOF
fi

# Create systemd service for detection worker
echo "Creating systemd service..."
sudo tee /etc/systemd/system/detection.service > /dev/null << EOF
[Unit]
Description=Sadie GTM Detection Consumer
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/sadie-gtm
ExecStart=/home/ubuntu/.local/bin/uv run python workflows/detection_consumer.py --concurrency 8
Restart=always
RestartSec=10
Environment=HOME=/home/ubuntu
EnvironmentFile=/home/ubuntu/sadie-gtm/.env

[Install]
WantedBy=multi-user.target
EOF

# Enable and start service
sudo systemctl daemon-reload
sudo systemctl enable detection
sudo systemctl start detection

echo ""
echo "=== Setup Complete ==="
echo "Detection worker is running. Check status with: sudo systemctl status detection"
echo "View logs with: journalctl -u detection -f"
