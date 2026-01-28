#!/bin/bash
# Distributed RMS scan across EC2 instances
#
# Each instance scans a different ID range to avoid rate limiting.
# Results are saved directly to the shared database.
#
# Usage:
#   # Show what would be scanned:
#   ./scripts/run/run_rms_distributed.sh ranges
#
#   # Deploy to all instances and run:
#   ./scripts/run/run_rms_distributed.sh deploy
#
#   # Check status:
#   ./scripts/run/run_rms_distributed.sh status
#
#   # Run locally for a specific instance number (1-7):
#   ./scripts/run/run_rms_distributed.sh local 3

set -e

# Configuration
TOTAL_IDS=25000  # Scan IDs 1-25000
IDS_PER_INSTANCE=3600  # ~3600 IDs per instance

# SSH config (matches existing ec2_status.sh)
SSH_KEY="$HOME/.ssh/m3-air.pem"
SSH_USER="ubuntu"
AWS_REGION="eu-north-1"

# Get EC2 IPs dynamically from AWS
get_ec2_ips() {
    aws ec2 describe-instances \
        --region "$AWS_REGION" \
        --filters "Name=instance-state-name,Values=running" "Name=tag:Project,Values=sadie-gtm" \
        --query 'Reservations[*].Instances[*].PublicIpAddress' \
        --output text 2>/dev/null
}

# Calculate range for instance N (1-indexed)
get_range() {
    local instance_num=$1
    local num_instances=$2
    local start=$(( (instance_num - 1) * IDS_PER_INSTANCE + 1 ))
    local end=$(( instance_num * IDS_PER_INSTANCE ))
    
    # Last instance gets any remaining IDs
    if [ "$instance_num" -eq "$num_instances" ]; then
        end=$TOTAL_IDS
    fi
    
    echo "$start $end"
}

# Deploy and run on all instances
deploy() {
    local IPS=($(get_ec2_ips))
    local NUM_INSTANCES=${#IPS[@]}
    
    if [ "$NUM_INSTANCES" -eq 0 ]; then
        echo "ERROR: No running EC2 instances found with tag Project=sadie-gtm"
        exit 1
    fi
    
    echo "Found $NUM_INSTANCES running EC2 instances"
    echo "Total IDs to scan: 1-$TOTAL_IDS"
    echo ""
    
    local i=1
    for ip in "${IPS[@]}"; do
        local range=($(get_range $i $NUM_INSTANCES))
        local start=${range[0]}
        local end=${range[1]}
        
        echo "Instance $i ($ip): IDs $start-$end"
        
        # Run in background via SSH
        ssh -i "$SSH_KEY" -o ConnectTimeout=10 -o StrictHostKeyChecking=no "$SSH_USER@$ip" \
            "cd ~/sadie_gtm_reverse_lookup && \
             git pull && \
             nohup uv run python -m workflows.scan_rms \
                --start $start --end $end \
                --concurrency 10 --delay 0.2 \
                --save-db \
                > /tmp/rms_scan.log 2>&1 &" &
        
        ((i++))
    done
    
    wait
    echo ""
    echo "All instances started!"
    echo "Check status with: $0 status"
    echo "View logs with: $0 logs"
}

# Check status on all instances
status() {
    local IPS=($(get_ec2_ips))
    
    echo "=== RMS Scan Status ==="
    echo ""
    
    for ip in "${IPS[@]}"; do
        echo -n "$ip: "
        
        # Check if scan is running
        running=$(ssh -i "$SSH_KEY" -o ConnectTimeout=5 -o StrictHostKeyChecking=no "$SSH_USER@$ip" \
            "pgrep -f 'scan_rms' >/dev/null && echo 'RUNNING' || echo 'STOPPED'" 2>/dev/null)
        
        if [ -z "$running" ]; then
            echo "UNREACHABLE"
        else
            # Get last log line
            last_log=$(ssh -i "$SSH_KEY" -o ConnectTimeout=5 -o StrictHostKeyChecking=no "$SSH_USER@$ip" \
                "tail -1 /tmp/rms_scan.log 2>/dev/null" 2>/dev/null || echo "")
            echo "$running - $last_log"
        fi
    done
}

# View logs from all instances
logs() {
    local IPS=($(get_ec2_ips))
    
    for ip in "${IPS[@]}"; do
        echo "=== $ip ==="
        ssh -i "$SSH_KEY" -o ConnectTimeout=5 -o StrictHostKeyChecking=no "$SSH_USER@$ip" \
            "tail -20 /tmp/rms_scan.log 2>/dev/null" 2>/dev/null || echo "No log"
        echo ""
    done
}

# Run locally for testing (specify instance number)
local_run() {
    local instance_num=${1:-1}
    local num_instances=${2:-7}
    local range=($(get_range $instance_num $num_instances))
    local start=${range[0]}
    local end=${range[1]}
    
    echo "Running as instance $instance_num of $num_instances: IDs $start-$end"
    
    cd "$(dirname "$0")/../.."
    uv run python -m workflows.scan_rms \
        --start "$start" --end "$end" \
        --concurrency 10 --delay 0.2 \
        --save-db
}

# Show ranges without running
show_ranges() {
    local IPS=($(get_ec2_ips))
    local NUM_INSTANCES=${#IPS[@]}
    
    if [ "$NUM_INSTANCES" -eq 0 ]; then
        NUM_INSTANCES=7
        echo "(No running instances found, showing plan for 7 instances)"
    else
        echo "Found $NUM_INSTANCES running instances"
    fi
    
    echo ""
    echo "RMS Scan Distribution Plan"
    echo "=========================="
    echo "Total IDs: 1-$TOTAL_IDS"
    echo ""
    
    for i in $(seq 1 $NUM_INSTANCES); do
        local range=($(get_range $i $NUM_INSTANCES))
        local count=$((${range[1]} - ${range[0]} + 1))
        if [ "$NUM_INSTANCES" -gt 0 ] && [ "$i" -le "${#IPS[@]}" ]; then
            echo "Instance $i (${IPS[$((i-1))]}): IDs ${range[0]}-${range[1]} ($count IDs)"
        else
            echo "Instance $i: IDs ${range[0]}-${range[1]} ($count IDs)"
        fi
    done
    
    echo ""
    echo "Estimated time per instance: ~12 minutes (at 10 conc, 0.2s delay)"
    echo "Expected yield: ~2-3% hit rate = 500-750 properties total"
}

# Stop scans on all instances
stop() {
    local IPS=($(get_ec2_ips))
    
    echo "Stopping RMS scans on all instances..."
    
    for ip in "${IPS[@]}"; do
        echo -n "$ip: "
        ssh -i "$SSH_KEY" -o ConnectTimeout=5 -o StrictHostKeyChecking=no "$SSH_USER@$ip" \
            "pkill -f 'scan_rms' && echo 'Stopped' || echo 'Not running'" 2>/dev/null || echo "Failed"
    done
}

# Main
case "${1:-help}" in
    deploy)
        deploy
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
    local)
        local_run "${2:-1}" "${3:-7}"
        ;;
    ranges)
        show_ranges
        ;;
    *)
        echo "Distributed RMS Scanner"
        echo ""
        echo "Usage:"
        echo "  $0 ranges   - Show ID ranges for each instance"
        echo "  $0 deploy   - Deploy and start scan on all EC2 instances"
        echo "  $0 status   - Check scan progress on all instances"
        echo "  $0 logs     - View logs from all instances"
        echo "  $0 stop     - Stop scans on all instances"
        echo "  $0 local N  - Run locally as instance N (for testing)"
        echo ""
        echo "EC2 instances are discovered automatically via AWS CLI."
        echo "Make sure instances have tag Project=sadie-gtm"
        ;;
esac
