#!/bin/bash
# RMS scan using SQS for work distribution
#
# Much cleaner than manual range splitting:
# 1. Enqueue all ID ranges to SQS
# 2. Start consumers on EC2 instances
# 3. Workers pull jobs automatically
#
# Usage:
#   ./scripts/run/run_rms_sqs.sh enqueue     # Send jobs to queue
#   ./scripts/run/run_rms_sqs.sh start       # Start consumers on all EC2
#   ./scripts/run/run_rms_sqs.sh status      # Check progress
#   ./scripts/run/run_rms_sqs.sh stop        # Stop all consumers

set -e

# Config
SSH_KEY="$HOME/.ssh/m3-air.pem"
SSH_USER="ubuntu"
AWS_REGION="eu-north-1"

get_ec2_ips() {
    aws ec2 describe-instances \
        --region "$AWS_REGION" \
        --filters "Name=instance-state-name,Values=running" "Name=tag:Project,Values=sadie-gtm" \
        --query 'Reservations[*].Instances[*].PublicIpAddress' \
        --output text 2>/dev/null
}

enqueue() {
    echo "Enqueueing RMS scan jobs to SQS..."
    uv run python -m workflows.rms_enqueue --start 1 --end 25000 --chunk-size 500
}

start_consumers() {
    local IPS=($(get_ec2_ips))
    
    if [ ${#IPS[@]} -eq 0 ]; then
        echo "ERROR: No running EC2 instances found"
        exit 1
    fi
    
    echo "Starting RMS consumers on ${#IPS[@]} instances..."
    
    for ip in "${IPS[@]}"; do
        echo "Starting consumer on $ip..."
        ssh -i "$SSH_KEY" -o ConnectTimeout=10 -o StrictHostKeyChecking=no "$SSH_USER@$ip" \
            "cd ~/sadie_gtm_reverse_lookup && \
             git pull -q && \
             pkill -f 'rms_consumer' 2>/dev/null || true && \
             nohup uv run python -m workflows.rms_consumer \
                --concurrency 10 --delay 0.2 \
                > /tmp/rms_consumer.log 2>&1 &" &
    done
    
    wait
    echo ""
    echo "All consumers started!"
    echo "Check progress: $0 status"
}

status() {
    local IPS=($(get_ec2_ips))
    
    echo "=== Queue Status ==="
    uv run python -m workflows.rms_enqueue --status 2>/dev/null || echo "Could not check queue"
    
    echo ""
    echo "=== Consumer Status ==="
    
    for ip in "${IPS[@]}"; do
        echo -n "$ip: "
        
        running=$(ssh -i "$SSH_KEY" -o ConnectTimeout=5 -o StrictHostKeyChecking=no "$SSH_USER@$ip" \
            "pgrep -f 'rms_consumer' >/dev/null && echo 'RUNNING' || echo 'STOPPED'" 2>/dev/null || echo "UNREACHABLE")
        
        if [ "$running" = "RUNNING" ]; then
            last_log=$(ssh -i "$SSH_KEY" -o ConnectTimeout=5 -o StrictHostKeyChecking=no "$SSH_USER@$ip" \
                "tail -1 /tmp/rms_consumer.log 2>/dev/null" 2>/dev/null | head -c 80)
            echo "$running - $last_log"
        else
            echo "$running"
        fi
    done
}

logs() {
    local IPS=($(get_ec2_ips))
    
    for ip in "${IPS[@]}"; do
        echo "=== $ip ==="
        ssh -i "$SSH_KEY" -o ConnectTimeout=5 -o StrictHostKeyChecking=no "$SSH_USER@$ip" \
            "tail -30 /tmp/rms_consumer.log 2>/dev/null" 2>/dev/null || echo "No log"
        echo ""
    done
}

stop() {
    local IPS=($(get_ec2_ips))
    
    echo "Stopping RMS consumers..."
    
    for ip in "${IPS[@]}"; do
        echo -n "$ip: "
        ssh -i "$SSH_KEY" -o ConnectTimeout=5 -o StrictHostKeyChecking=no "$SSH_USER@$ip" \
            "pkill -f 'rms_consumer' && echo 'Stopped' || echo 'Not running'" 2>/dev/null || echo "Failed"
    done
}

# Main
case "${1:-help}" in
    enqueue)
        enqueue
        ;;
    start)
        start_consumers
        ;;
    status)
        status
        ;;
    logs)
        logs
        ;;
    stop)
        stop
        ;;
    run)
        # Full run: enqueue then start
        enqueue
        echo ""
        start_consumers
        ;;
    *)
        echo "RMS Scanner with SQS"
        echo ""
        echo "Usage:"
        echo "  $0 enqueue  - Send ID range jobs to SQS queue"
        echo "  $0 start    - Start consumers on all EC2 instances"
        echo "  $0 status   - Check queue and consumer status"
        echo "  $0 logs     - View consumer logs"
        echo "  $0 stop     - Stop all consumers"
        echo "  $0 run      - Enqueue + start (full run)"
        echo ""
        echo "Workers pull jobs from SQS automatically."
        echo "No manual range splitting needed!"
        ;;
esac
