#!/bin/bash
# Deploy to all Sadie GTM EC2 instances
#
# Usage:
#   ./scripts/deploy_all.sh pull          # Git pull on all servers
#   ./scripts/deploy_all.sh start-enrich  # Start name enrichment consumers
#   ./scripts/deploy_all.sh stop-enrich   # Stop name enrichment consumers
#   ./scripts/deploy_all.sh status        # Check status on all servers
#   ./scripts/deploy_all.sh enqueue       # Enqueue hotels for enrichment

set -e

KEY="$HOME/.ssh/m3-air.pem"
HOSTS=(
    "13.61.104.62"
    "13.60.58.185"
    "51.20.9.238"
    "13.61.7.197"
    "13.51.168.43"
    "13.53.197.203"
    "51.20.191.25"
)

run_on_all() {
    local cmd="$1"
    for host in "${HOSTS[@]}"; do
        echo "=== $host ==="
        ssh -i "$KEY" -o StrictHostKeyChecking=no ubuntu@$host "$cmd" || echo "Failed on $host"
        echo ""
    done
}

run_on_all_bg() {
    local cmd="$1"
    for host in "${HOSTS[@]}"; do
        echo "Starting on $host..."
        ssh -i "$KEY" -o StrictHostKeyChecking=no ubuntu@$host "cd ~/sadie-gtm && nohup $cmd > /var/log/sadie/enrich-names.log 2>&1 &" &
    done
    wait
    echo "All consumers started in background"
}

case "$1" in
    pull)
        echo "Pulling latest code on all servers..."
        run_on_all "cd ~/sadie-gtm && git pull"
        ;;
    
    start-enrich)
        echo "Starting name enrichment consumers on all servers..."
        run_on_all_bg "uv run python -m workflows.enrich_names_consumer"
        ;;
    
    stop-enrich)
        echo "Stopping name enrichment consumers on all servers..."
        run_on_all "pkill -f 'enrich_names_consumer' || true"
        ;;
    
    status)
        echo "Checking status on all servers..."
        run_on_all "ps aux | grep -E 'enrich_names|detection' | grep -v grep || echo 'No workers running'"
        ;;
    
    enqueue)
        echo "Enqueueing hotels for name enrichment (from first server)..."
        ssh -i "$KEY" ubuntu@${HOSTS[0]} "cd ~/sadie-gtm && uv run python -m workflows.enrich_names_enqueue --limit 10000"
        ;;
    
    logs)
        echo "Tailing logs on first server (Ctrl+C to stop)..."
        ssh -i "$KEY" ubuntu@${HOSTS[0]} "tail -f /var/log/sadie/enrich-names.log"
        ;;
    
    *)
        echo "Usage: $0 {pull|start-enrich|stop-enrich|status|enqueue|logs}"
        echo ""
        echo "Commands:"
        echo "  pull          - Git pull on all servers"
        echo "  start-enrich  - Start name enrichment consumers on all servers"
        echo "  stop-enrich   - Stop name enrichment consumers"
        echo "  status        - Check running workers"
        echo "  enqueue       - Queue hotels for name enrichment"
        echo "  logs          - Tail logs from first server"
        exit 1
        ;;
esac
