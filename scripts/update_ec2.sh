#!/bin/bash
# Update code and restart workers on all EC2 instances
# Usage: ./scripts/update_ec2.sh [--no-restart]
#
# Gets IPs dynamically from AWS - no hardcoded values

set -e

KEY="$HOME/.ssh/m3-air.pem"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
RESTART=true

if [ "$1" == "--no-restart" ]; then
    RESTART=false
fi

# Get running instance IPs from AWS
echo "Fetching EC2 instance IPs from AWS..."
IPS=$(aws ec2 describe-instances \
    --region eu-north-1 \
    --filters "Name=instance-state-name,Values=running" "Name=tag:Project,Values=sadie-gtm" \
    --query 'Reservations[*].Instances[*].PublicIpAddress' \
    --output text 2>/dev/null)

# Fallback to zshrc if AWS query fails or returns empty
if [ -z "$IPS" ]; then
    echo "AWS query returned no results, falling back to ~/.zshrc..."
    IPS=$(grep "^alias ip" ~/.zshrc | sed 's/alias ip[0-9]*=//' | tr '\n' ' ')
fi

if [ -z "$IPS" ]; then
    echo "ERROR: No instance IPs found"
    exit 1
fi

echo "Found instances: $IPS"
echo ""

for ip in $IPS; do
    echo "=== $ip ==="

    # Sync code
    rsync -avz --delete \
        --exclude '.venv' \
        --exclude '.git' \
        --exclude '__pycache__' \
        --exclude '*.pyc' \
        --exclude '.env' \
        --exclude '*.xlsx' \
        -e "ssh -i $KEY -o ConnectTimeout=10 -o StrictHostKeyChecking=no" \
        "$PROJECT_DIR/" ubuntu@$ip:~/sadie-gtm/ 2>/dev/null \
        && echo "  Synced" || { echo "  SYNC FAILED"; continue; }

    # Fix .env (remove export statements) and update systemd service
    ssh -i "$KEY" -o ConnectTimeout=10 -o StrictHostKeyChecking=no ubuntu@$ip '
        # Fix .env file - remove export statements
        if [ -f ~/sadie-gtm/.env ]; then
            sed -i "s/^export //" ~/sadie-gtm/.env
            echo "  Fixed .env"
        fi

        # Always update systemd service (includes EnvironmentFile now)
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
        sudo systemctl daemon-reload
        sudo systemctl enable detection
        echo "  Updated systemd service"
    ' 2>/dev/null

    # Restart service if requested
    if [ "$RESTART" == "true" ]; then
        ssh -i "$KEY" -o ConnectTimeout=10 -o StrictHostKeyChecking=no ubuntu@$ip \
            "sudo systemctl restart detection && echo '  Restarted'" 2>/dev/null || echo "  Restart failed"
    fi

    echo ""
done

echo "=== Done ==="
