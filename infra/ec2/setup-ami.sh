#!/bin/bash
# =============================================================================
# Sadie GTM EC2 AMI Setup Script
# =============================================================================
# Run this on a fresh Ubuntu 22.04 EC2 instance to create an AMI
#
# Usage:
#   curl -sSL https://raw.githubusercontent.com/.../setup-ami.sh | bash
#   # OR
#   ./setup-ami.sh
#
# After running, create an AMI from the instance. New instances from that AMI
# will auto-start the detection consumer and cron jobs on boot.
# =============================================================================

set -e

echo "=============================================="
echo "Sadie GTM EC2 AMI Setup"
echo "=============================================="

# Update system
echo "[1/8] Updating system..."
sudo apt-get update -qq
sudo apt-get upgrade -y -qq

# Install system dependencies
echo "[2/8] Installing system dependencies..."
sudo apt-get install -y -qq \
    python3.11 \
    python3.11-venv \
    python3-pip \
    git \
    curl \
    wget \
    jq \
    chromium-browser \
    chromium-chromedriver \
    libnss3 \
    libatk-bridge2.0-0 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2

# Install uv (fast Python package manager)
echo "[3/8] Installing uv..."
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"

# Clone repository
echo "[4/8] Cloning repository..."
cd /home/ubuntu
if [ -d "sadie-gtm" ]; then
    cd sadie-gtm && git pull
else
    git clone https://github.com/aliabbassi-1337/sadie-gtm.git sadie-gtm
    cd sadie-gtm
fi

# Install Python dependencies
echo "[5/8] Installing Python dependencies..."
uv sync

# Install Playwright browsers
echo "[6/8] Installing Playwright browsers..."
uv run playwright install chromium
uv run playwright install-deps chromium

# Create log directory
echo "[7/8] Setting up logging..."
sudo mkdir -p /var/log/sadie
sudo chown ubuntu:ubuntu /var/log/sadie

# Configure logrotate
sudo tee /etc/logrotate.d/sadie > /dev/null <<EOF
/var/log/sadie/*.log {
    daily
    missingok
    rotate 7
    compress
    delaycompress
    notifempty
    create 0644 ubuntu ubuntu
}
EOF

# Generate and install systemd service + cron
echo "[8/8] Generating and installing systemd service and cron..."
uv run python scripts/deploy_ec2.py generate

# Install systemd services
for f in infra/ec2/generated/*.service; do
    sudo cp "$f" /etc/systemd/system/
done
sudo systemctl daemon-reload
sudo systemctl enable detection-consumer.service

# Install cron jobs
sudo cp infra/ec2/generated/sadie-cron /etc/cron.d/sadie-gtm
sudo chmod 644 /etc/cron.d/sadie-gtm

echo ""
echo "=============================================="
echo "Setup Complete!"
echo "=============================================="
echo ""
echo "IMPORTANT: Create .env file before starting services:"
echo "  nano /home/ubuntu/sadie-gtm/.env"
echo ""
echo "Required environment variables:"
echo "  DATABASE_URL=postgresql://..."
echo "  SERPER_API_KEY=..."
echo "  GROQ_API_KEY=..."
echo "  SQS_DETECTION_QUEUE_URL=..."
echo "  SLACK_WEBHOOK_URL=..."
echo ""
echo "Then start the detection consumer:"
echo "  sudo systemctl start detection-consumer"
echo ""
echo "Check status:"
echo "  sudo systemctl status detection-consumer"
echo "  sudo journalctl -u detection-consumer -f"
echo ""
echo "Cron jobs will start automatically."
echo "View logs: tail -f /var/log/sadie/*.log"
echo ""
echo "Once .env is configured, create an AMI from this instance."
echo "New instances from that AMI will auto-start on boot."
