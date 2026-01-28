#!/bin/bash
# Fetches EC2 instance IPs and updates SSH aliases in zshrc

REGION="eu-north-1"
KEY_PATH="~/.ssh/m3-air.pem"

# Get all running instances with their IPs
echo "Fetching EC2 instances from $REGION..."
IPS=$(aws ec2 describe-instances \
    --region $REGION \
    --filters "Name=instance-state-name,Values=running" \
    --query "Reservations[*].Instances[*].PublicIpAddress" \
    --output text | tr '\t' '\n' | grep -v "^$" | sort)

if [ -z "$IPS" ]; then
    echo "No running instances found"
    exit 1
fi

# Generate aliases
echo ""
echo "# Sadie GTM SSH aliases (auto-generated $(date +%Y-%m-%d))"
i=1
for ip in $IPS; do
    echo "alias sadie-gtm$i='ssh -i $KEY_PATH ubuntu@$ip'"
    echo "alias ip$i=$ip"
    ((i++))
done

echo ""
echo "# Deploy IPs for GitHub Actions"
echo "# IPS=\"$(echo $IPS | tr '\n' ' ')\""
