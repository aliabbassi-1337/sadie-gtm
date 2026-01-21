#!/usr/bin/env python3
"""
Deploy and setup EC2 instances for detection workers.

Usage:
    # First-time setup (installs uv, playwright, clones repo, copies .env)
    uv run python scripts/deploy_ec2.py setup --host ubuntu@1.2.3.4 --key ~/.ssh/key.pem

    # Deploy systemd services and cron jobs from workflows.yaml
    uv run python scripts/deploy_ec2.py deploy --host ubuntu@1.2.3.4 --key ~/.ssh/key.pem

    # Deploy and restart services
    uv run python scripts/deploy_ec2.py deploy --host ubuntu@1.2.3.4 --key ~/.ssh/key.pem --restart

    # Generate config files locally (for review)
    uv run python scripts/deploy_ec2.py generate
"""

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Dict, Any

import yaml
from loguru import logger


# Paths
PROJECT_ROOT = Path(__file__).parent.parent
WORKFLOWS_YAML = PROJECT_ROOT / "workflows.yaml"
OUTPUT_DIR = PROJECT_ROOT / "infra" / "ec2" / "generated"

# Templates
SYSTEMD_TEMPLATE = """[Unit]
Description={description}
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=ubuntu
Group=ubuntu
WorkingDirectory=/home/ubuntu/sadie-gtm
EnvironmentFile=/home/ubuntu/sadie-gtm/.env

ExecStart=/home/ubuntu/.local/bin/{command}

Restart=on-failure
RestartSec=30
StartLimitIntervalSec=300
StartLimitBurst=5

StandardOutput=journal
StandardError=journal
SyslogIdentifier={name}

[Install]
WantedBy=multi-user.target
"""

CRON_HEADER = """# Sadie GTM Cron Jobs
# Auto-generated from workflows.yaml - DO NOT EDIT DIRECTLY
# Regenerate with: uv run python scripts/deploy_ec2.py generate

SHELL=/bin/bash
PATH=/usr/local/bin:/usr/bin:/bin:/home/ubuntu/.local/bin
HOME=/home/ubuntu
MAILTO=""

"""

CRON_JOB_TEMPLATE = "# {description}\n{schedule} ubuntu cd /home/ubuntu/sadie-gtm && source .env && /home/ubuntu/.local/bin/{command} >> /var/log/sadie/{name}.log 2>&1\n"


def load_workflows() -> Dict[str, Any]:
    """Load workflows.yaml."""
    with open(WORKFLOWS_YAML) as f:
        return yaml.safe_load(f)


def generate_systemd_services(ec2_workflows: Dict[str, Any]) -> Dict[str, str]:
    """Generate systemd service files for continuous workers."""
    services = {}
    
    for name, config in ec2_workflows.items():
        if config.get("type") != "systemd":
            continue
        
        service_name = name.replace("_", "-")
        content = SYSTEMD_TEMPLATE.format(
            name=service_name,
            description=config.get("description", name),
            command=config["command"],
        )
        services[f"{service_name}.service"] = content
    
    return services


def generate_cron_file(ec2_workflows: Dict[str, Any]) -> str:
    """Generate cron file for scheduled jobs."""
    content = CRON_HEADER
    
    for name, config in ec2_workflows.items():
        if config.get("type") != "cron":
            continue
        
        job_name = name.replace("_", "-")
        content += CRON_JOB_TEMPLATE.format(
            name=job_name,
            description=config.get("description", name),
            schedule=config["schedule"],
            command=config["command"],
        )
        content += "\n"
    
    return content


def generate_all() -> None:
    """Generate all config files from workflows.yaml."""
    workflows = load_workflows()
    ec2_workflows = workflows.get("ec2", {})
    
    if not ec2_workflows:
        logger.error("No EC2 workflows found in workflows.yaml")
        sys.exit(1)
    
    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Generate systemd services
    services = generate_systemd_services(ec2_workflows)
    for filename, content in services.items():
        path = OUTPUT_DIR / filename
        path.write_text(content)
        logger.info(f"Generated: {path}")
    
    # Generate cron file
    cron_content = generate_cron_file(ec2_workflows)
    cron_path = OUTPUT_DIR / "sadie-cron"
    cron_path.write_text(cron_content)
    logger.info(f"Generated: {cron_path}")
    
    logger.info("")
    logger.info("Generated files in infra/ec2/generated/")
    logger.info("Review and deploy with: uv run python scripts/deploy_ec2.py deploy --host <ec2-host>")


def run_ssh(host: str, cmd: str, key: str = None, check: bool = True) -> subprocess.CompletedProcess:
    """Run SSH command."""
    ssh_args = ["ssh", "-o", "StrictHostKeyChecking=no"]
    if key:
        ssh_args.extend(["-i", key])
    return subprocess.run(ssh_args + [host, cmd], check=check)


def run_scp(host: str, local_path: str, remote_path: str, key: str = None) -> None:
    """Copy file via SCP."""
    scp_args = ["scp", "-o", "StrictHostKeyChecking=no"]
    if key:
        scp_args.extend(["-i", key])
    subprocess.run(scp_args + [local_path, f"{host}:{remote_path}"], check=True)


def setup(host: str, key: str = None) -> None:
    """First-time setup of EC2 instance.

    Installs uv, clones repo, installs dependencies, playwright, and copies .env and AWS credentials.
    """
    logger.info(f"Setting up {host}...")

    # Install uv
    logger.info("Installing uv...")
    run_ssh(host, "curl -LsSf https://astral.sh/uv/install.sh | sh", key)

    # Sync project files
    logger.info("Syncing project files...")
    rsync_args = [
        "rsync", "-avz", "--delete",
        "--exclude", ".venv",
        "--exclude", ".git",
        "--exclude", "__pycache__",
        "--exclude", "*.pyc",
        "--exclude", ".env",  # will copy separately
        "-e", f"ssh -o StrictHostKeyChecking=no{' -i ' + key if key else ''}",
        str(PROJECT_ROOT) + "/",
        f"{host}:~/sadie-gtm/"
    ]
    subprocess.run(rsync_args, check=True)

    # Install Python dependencies
    logger.info("Installing Python dependencies...")
    run_ssh(host, "cd ~/sadie-gtm && ~/.local/bin/uv sync", key)

    # Install Playwright browsers
    logger.info("Installing Playwright browsers...")
    run_ssh(host, "cd ~/sadie-gtm && ~/.local/bin/uv run playwright install chromium", key)

    # Install Playwright system dependencies (needs sudo)
    logger.info("Installing Playwright system dependencies...")
    run_ssh(host, "cd ~/sadie-gtm && sudo ~/.local/bin/uv run playwright install-deps chromium", key)

    # Copy .env from local
    logger.info("Copying .env...")
    local_env = PROJECT_ROOT / ".env"
    if local_env.exists():
        run_scp(host, str(local_env), "~/sadie-gtm/.env", key)
    else:
        logger.warning("No local .env found, skipping")

    # Copy AWS credentials
    logger.info("Copying AWS credentials...")
    aws_creds = Path.home() / ".aws" / "credentials"
    if aws_creds.exists():
        run_ssh(host, "mkdir -p ~/.aws", key)
        run_scp(host, str(aws_creds), "~/.aws/credentials", key)
    else:
        logger.warning("No AWS credentials found, skipping")

    # Create log directory
    run_ssh(host, "sudo mkdir -p /var/log/sadie && sudo chown ubuntu:ubuntu /var/log/sadie", key)

    logger.info("")
    logger.info("Setup complete!")
    logger.info(f"Test with: ssh {'-i ' + key + ' ' if key else ''}{host} 'cd ~/sadie-gtm && ~/.local/bin/uv run python -c \"print(1)\"'")


def deploy(host: str, restart: bool = False, key: str = None) -> None:
    """Deploy generated configs to EC2 via SSH."""
    # First generate the files
    generate_all()

    logger.info(f"Deploying to {host}...")

    # Copy generated files
    generated_files = list(OUTPUT_DIR.glob("*"))
    if not generated_files:
        logger.error("No generated files found. Run 'generate' first.")
        sys.exit(1)

    # Create remote directories
    run_ssh(host, "sudo mkdir -p /var/log/sadie && sudo chown ubuntu:ubuntu /var/log/sadie", key)

    # Copy files to temp location on remote
    for f in generated_files:
        logger.info(f"  Copying {f.name}...")
        run_scp(host, str(f), f"/tmp/{f.name}", key)

    # Move to correct locations and set permissions
    commands = []

    # Systemd services
    for f in generated_files:
        if f.suffix == ".service":
            commands.append(f"sudo mv /tmp/{f.name} /etc/systemd/system/{f.name}")
            commands.append(f"sudo chmod 644 /etc/systemd/system/{f.name}")

    # Cron file
    if (OUTPUT_DIR / "sadie-cron").exists():
        commands.append("sudo mv /tmp/sadie-cron /etc/cron.d/sadie-gtm")
        commands.append("sudo chmod 644 /etc/cron.d/sadie-gtm")

    # Reload systemd
    commands.append("sudo systemctl daemon-reload")

    # Enable services
    for f in generated_files:
        if f.suffix == ".service":
            service_name = f.stem
            commands.append(f"sudo systemctl enable {service_name}")

    # Run commands
    for cmd in commands:
        logger.info(f"  {cmd}")
        run_ssh(host, cmd, key)

    # Restart if requested
    if restart:
        logger.info("Restarting services...")
        for f in generated_files:
            if f.suffix == ".service":
                service_name = f.stem
                run_ssh(host, f"sudo systemctl restart {service_name}", key)
                logger.info(f"  Restarted {service_name}")

    logger.info("")
    logger.info("Deployment complete!")
    logger.info("")
    logger.info("Check status:")
    key_arg = f"-i {key} " if key else ""
    for f in generated_files:
        if f.suffix == ".service":
            logger.info(f"  ssh {key_arg}{host} sudo systemctl status {f.stem}")
    logger.info(f"  ssh {key_arg}{host} cat /etc/cron.d/sadie-gtm")


def main():
    parser = argparse.ArgumentParser(
        description="Deploy and setup EC2 instances for detection workers",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Setup command (first-time instance setup)
    setup_parser = subparsers.add_parser("setup", help="First-time setup of EC2 instance")
    setup_parser.add_argument("--host", required=True, help="SSH host (e.g., ubuntu@1.2.3.4)")
    setup_parser.add_argument("--key", "-i", help="Path to SSH private key (e.g., ~/.ssh/my-key.pem)")

    # Generate command
    subparsers.add_parser("generate", help="Generate config files locally")

    # Deploy command
    deploy_parser = subparsers.add_parser("deploy", help="Deploy systemd services and cron to EC2")
    deploy_parser.add_argument("--host", required=True, help="SSH host (e.g., ubuntu@1.2.3.4)")
    deploy_parser.add_argument("--key", "-i", help="Path to SSH private key (e.g., ~/.ssh/my-key.pem)")
    deploy_parser.add_argument("--restart", action="store_true", help="Restart services after deploy")

    args = parser.parse_args()

    # Configure logging
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")

    if args.command == "setup":
        setup(args.host, args.key)
    elif args.command == "generate":
        generate_all()
    elif args.command == "deploy":
        deploy(args.host, args.restart, args.key)


if __name__ == "__main__":
    main()
