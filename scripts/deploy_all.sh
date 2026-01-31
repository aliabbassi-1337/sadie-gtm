#!/bin/bash
# Deploy to all Sadie GTM EC2 instances
#
# Usage:
#   ./scripts/deploy_all.sh deploy        # Full deploy: pull, generate, install systemd/cron
#   ./scripts/deploy_all.sh pull          # Git pull on all servers
#   ./scripts/deploy_all.sh status        # Check systemd status on all servers
#   ./scripts/deploy_all.sh logs          # Tail logs from first server

set -e

KEY="$HOME/.ssh/m3-air.pem"
PRIMARY_HOST="13.61.104.62"  # First server runs cron jobs (enqueuer)
HOSTS=(
    "13.61.104.62"
    "13.60.58.185"
    "51.20.9.238"
    "13.60.236.93"
    "16.171.174.5"
    "13.53.197.203"
    "51.20.191.25"
)

# Source uv path on remote
REMOTE_PREFIX="source ~/.local/bin/env 2>/dev/null || export PATH=\$HOME/.local/bin:\$PATH"

run_on_all() {
    local cmd="$1"
    for host in "${HOSTS[@]}"; do
        echo "=== $host ==="
        ssh -i "$KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=10 ubuntu@$host "$REMOTE_PREFIX && $cmd" || echo "Failed on $host"
        echo ""
    done
}

run_on_primary() {
    local cmd="$1"
    echo "=== $PRIMARY_HOST (primary) ==="
    ssh -i "$KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=10 ubuntu@$PRIMARY_HOST "$REMOTE_PREFIX && $cmd"
}

case "$1" in
    deploy)
        echo "=========================================="
        echo "Full deploy to all 7 EC2 instances"
        echo "=========================================="
        
        echo ""
        echo "[1/4] Pulling latest code..."
        run_on_all "cd ~/sadie-gtm && git pull"
        
        echo ""
        echo "[2/4] Generating systemd + cron files..."
        run_on_all "cd ~/sadie-gtm && uv run python scripts/deploy_ec2.py generate"
        
        echo ""
        echo "[3/4] Installing systemd services on all servers..."
        run_on_all "cd ~/sadie-gtm && \
            sudo cp infra/ec2/generated/*.service /etc/systemd/system/ 2>/dev/null || true && \
            sudo systemctl daemon-reload && \
            sudo systemctl enable booking-enrichment-consumer 2>/dev/null || true && \
            sudo systemctl restart booking-enrichment-consumer 2>/dev/null || true"
        
        echo ""
        echo "[4/4] Installing cron on PRIMARY server only..."
        run_on_primary "sudo cp ~/sadie-gtm/infra/ec2/generated/sadie-cron /etc/cron.d/sadie-gtm && sudo chmod 644 /etc/cron.d/sadie-gtm"
        
        echo ""
        echo "=========================================="
        echo "Deploy complete!"
        echo "=========================================="
        echo ""
        echo "Consumer: running on all 7 servers (systemd)"
        echo "Enqueuer: cron on primary server ($PRIMARY_HOST) every 10 min"
        ;;
    
    pull)
        echo "Pulling latest code on all servers..."
        run_on_all "cd ~/sadie-gtm && git pull"
        ;;
    
    status)
        echo "Checking systemd status on all servers..."
        run_on_all "systemctl status booking-enrichment-consumer --no-pager 2>/dev/null | head -5 || echo 'Service not installed'"
        ;;
    
    logs)
        echo "Tailing enrichment logs on first server (Ctrl+C to stop)..."
        ssh -i "$KEY" ubuntu@$PRIMARY_HOST "sudo journalctl -u booking-enrichment-consumer -f"
        ;;
    
    enqueue)
        echo "Manually enqueueing hotels (from primary server)..."
        run_on_primary "cd ~/sadie-gtm && uv run python -m workflows.enrich_booking_pages_enqueue --limit 10000"
        ;;
    
    push-env)
        if [ -z "$2" ]; then
            echo "Usage: $0 push-env 'VAR_NAME=value'"
            echo "Example: $0 push-env 'SQS_BOOKING_ENRICHMENT_QUEUE_URL=https://...'"
            exit 1
        fi
        echo "Pushing env var to all servers..."
        VAR_NAME=$(echo "$2" | cut -d= -f1)
        run_on_all "grep -q $VAR_NAME ~/sadie-gtm/.env || echo '$2' >> ~/sadie-gtm/.env && grep $VAR_NAME ~/sadie-gtm/.env"
        ;;
    
    *)
        echo "Usage: $0 {deploy|pull|status|logs|enqueue|push-env}"
        echo ""
        echo "Commands:"
        echo "  deploy   - Full deploy: pull, generate, install systemd/cron"
        echo "  pull     - Git pull on all servers"
        echo "  status   - Check systemd service status"
        echo "  logs     - Tail consumer logs"
        echo "  enqueue  - Manually enqueue hotels for enrichment"
        exit 1
        ;;
esac
