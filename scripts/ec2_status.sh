#!/bin/bash
# Check status of all EC2 workers
# Usage: ./scripts/ec2_status.sh
#
# Gets IPs dynamically from AWS

KEY="$HOME/.ssh/m3-air.pem"

# Get running instance IPs from AWS
IPS=$(aws ec2 describe-instances \
    --region eu-north-1 \
    --filters "Name=instance-state-name,Values=running" "Name=tag:Project,Values=sadie-gtm" \
    --query 'Reservations[*].Instances[*].PublicIpAddress' \
    --output text 2>/dev/null)

# Fallback to zshrc if AWS query fails
if [ -z "$IPS" ]; then
    IPS=$(grep "^alias ip" ~/.zshrc | sed 's/alias ip[0-9]*=//' | tr '\n' ' ')
fi

echo "=== EC2 Worker Status ==="
echo ""

for ip in $IPS; do
    echo -n "$ip: "
    result=$(ssh -i "$KEY" -o ConnectTimeout=5 -o StrictHostKeyChecking=no ubuntu@$ip \
        "systemctl is-active detection 2>/dev/null || echo 'no-service'" 2>/dev/null)

    if [ -z "$result" ]; then
        echo "UNREACHABLE"
    elif [ "$result" == "active" ]; then
        count=$(ssh -i "$KEY" -o ConnectTimeout=5 -o StrictHostKeyChecking=no ubuntu@$ip \
            "ps aux | grep -E 'python.*detection' | grep -v grep | wc -l" 2>/dev/null)
        echo "RUNNING ($count processes)"
    elif [ "$result" == "no-service" ]; then
        echo "NO SERVICE"
    else
        echo "STOPPED"
    fi
done

echo ""

# Check queue
echo "=== SQS Queue ==="
attrs=$(aws sqs get-queue-attributes \
    --queue-url "https://sqs.eu-north-1.amazonaws.com/760711518969/detection-queue" \
    --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible \
    --region eu-north-1 \
    --query 'Attributes.[ApproximateNumberOfMessages,ApproximateNumberOfMessagesNotVisible]' \
    --output text 2>/dev/null)

if [ -n "$attrs" ]; then
    waiting=$(echo "$attrs" | awk '{print $1}')
    inflight=$(echo "$attrs" | awk '{print $2}')
    echo "Waiting: $waiting | In-flight: $inflight"
else
    echo "Could not check queue"
fi
