#!/bin/bash
# Update code and restart workers on all EC2 instances
# Usage: ./scripts/update_ec2.sh [--restart]

set -e

# Read IPs from zshrc
IPS=$(grep "^alias ip" ~/.zshrc | sed 's/alias ip[0-9]*=//')

KEY="$HOME/.ssh/m3-air.pem"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
RESTART=${1:-"--restart"}  # Default to restart

echo "=== Syncing code to all EC2 instances ==="
echo "Source: $PROJECT_DIR"
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

    # Restart service if requested
    if [ "$RESTART" == "--restart" ]; then
        ssh -i "$KEY" -o ConnectTimeout=10 -o StrictHostKeyChecking=no ubuntu@$ip \
            "sudo systemctl restart detection 2>/dev/null && echo '  Restarted' || echo '  No systemd service'" 2>/dev/null
    fi

    echo ""
done

echo "=== Done ==="
