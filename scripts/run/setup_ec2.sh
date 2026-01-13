#!/bin/bash
# EC2 Setup - Run once after cloning
set -e

# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.local/bin/env

# Install deps + playwright
uv sync
uv run playwright install chromium
uv run playwright install-deps

# Setup dirs
mkdir -p scraper_output/florida detector_output/florida

# Copy .env
cp .env.example .env
echo "Done! Edit .env then run: ./run_ec2_detect.sh"
