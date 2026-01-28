#!/bin/bash
# Distributed RMS Scanner
# Runs the RMS scanner across multiple EC2 instances

set -e

# Server IPs
SERVERS=(
    "13.61.104.62"
    "13.60.58.185"
    "51.20.9.238"
    "13.60.236.93"
    "13.51.168.43"
    "13.53.197.203"
    "51.20.191.25"
)

KEY="~/.ssh/m3-air.pem"
USER="ubuntu"
REMOTE_DIR="/home/ubuntu/sadie-gtm"

# Scan configuration
TOTAL_START=0
TOTAL_END=20000
ENGINE="old"  # old, new, or both

# Calculate range per server
NUM_SERVERS=${#SERVERS[@]}
RANGE_PER_SERVER=$(( (TOTAL_END - TOTAL_START) / NUM_SERVERS ))

echo "=== RMS Distributed Scanner ==="
echo "Total range: $TOTAL_START - $TOTAL_END"
echo "Servers: $NUM_SERVERS"
echo "Range per server: $RANGE_PER_SERVER"
echo ""

# Deploy and run on each server
for i in "${!SERVERS[@]}"; do
    SERVER="${SERVERS[$i]}"
    START=$(( TOTAL_START + (i * RANGE_PER_SERVER) ))
    END=$(( START + RANGE_PER_SERVER ))
    
    # Last server gets the remainder
    if [ $i -eq $(( NUM_SERVERS - 1 )) ]; then
        END=$TOTAL_END
    fi
    
    echo "=== Server $((i+1)): $SERVER (IDs $START - $END) ==="
    
    # Run scanner in background via SSH
    ssh -i $KEY -o StrictHostKeyChecking=no $USER@$SERVER "
        cd $REMOTE_DIR && \
        source .venv/bin/activate && \
        nohup python scripts/scrapers/rms_scanner.py \
            --start $START \
            --end $END \
            --engine $ENGINE \
            --concurrency 15 \
            --output rms_scan_${i}.json \
            --output-urls rms_urls_${i}.txt \
            > rms_scan_${i}.log 2>&1 &
        echo 'Started scanner for range $START - $END'
    " &
done

echo ""
echo "All scanners started! Monitor with:"
echo "  ssh -i $KEY $USER@<server> 'tail -f ~/sadie-gtm/rms_scan_*.log'"
echo ""
echo "After completion, collect results with:"
echo "  for s in \${SERVERS[@]}; do scp -i $KEY $USER@\$s:~/sadie-gtm/rms_*.json ./results/; done"

wait
echo "Done launching scanners!"
