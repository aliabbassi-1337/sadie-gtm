#!/usr/bin/env python3
"""
Deploy EC2 configuration from workflows.yaml

Reads workflows.yaml and generates:
- systemd service files for continuous workers
- cron file for scheduled jobs

Usage:
    # Generate config files locally (for review)
    uv run python scripts/deploy_ec2.py generate

    # Deploy to EC2 instance via SSH
    uv run python scripts/deploy_ec2.py deploy --host ec2-user@1.2.3.4

    # Deploy and restart services
    uv run python scripts/deploy_ec2.py deploy --host ec2-user@1.2.3.4 --restart
"""

import argparse
import subprocess
import sys
import tempfile
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


def deploy(host: str, restart: bool = False, key: str = None) -> None:
    """Deploy generated configs to EC2 via SSH."""
    # First generate the files
    generate_all()
    
    logger.info(f"Deploying to {host}...")
    
    # Build SSH/SCP args with optional key
    ssh_args = ["ssh"]
    scp_args = ["scp"]
    if key:
        ssh_args.extend(["-i", key])
        scp_args.extend(["-i", key])
    
    # Copy generated files
    generated_files = list(OUTPUT_DIR.glob("*"))
    if not generated_files:
        logger.error("No generated files found. Run 'generate' first.")
        sys.exit(1)
    
    # Create remote directories
    subprocess.run(
        ssh_args + [host, "sudo mkdir -p /var/log/sadie && sudo chown ubuntu:ubuntu /var/log/sadie"],
        check=True
    )
    
    # Copy files to temp location on remote
    for f in generated_files:
        logger.info(f"  Copying {f.name}...")
        subprocess.run(scp_args + [str(f), f"{host}:/tmp/{f.name}"], check=True)
    
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
        subprocess.run(ssh_args + [host, cmd], check=True)
    
    # Restart if requested
    if restart:
        logger.info("Restarting services...")
        for f in generated_files:
            if f.suffix == ".service":
                service_name = f.stem
                subprocess.run(ssh_args + [host, f"sudo systemctl restart {service_name}"], check=True)
                logger.info(f"  Restarted {service_name}")
    
    logger.info("")
    logger.info("Deployment complete!")
    logger.info("")
    logger.info("Check status:")
    for f in generated_files:
        if f.suffix == ".service":
            logger.info(f"  ssh {host} sudo systemctl status {f.stem}")
    logger.info(f"  ssh {host} cat /etc/cron.d/sadie-gtm")


def main():
    parser = argparse.ArgumentParser(
        description="Deploy EC2 configuration from workflows.yaml",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # Generate command
    subparsers.add_parser("generate", help="Generate config files locally")
    
    # Deploy command
    deploy_parser = subparsers.add_parser("deploy", help="Deploy to EC2 via SSH")
    deploy_parser.add_argument("--host", required=True, help="SSH host (e.g., ubuntu@1.2.3.4)")
    deploy_parser.add_argument("--key", "-i", help="Path to SSH private key (e.g., ~/.ssh/my-key.pem)")
    deploy_parser.add_argument("--restart", action="store_true", help="Restart services after deploy")
    
    args = parser.parse_args()
    
    # Configure logging
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")
    
    if args.command == "generate":
        generate_all()
    elif args.command == "deploy":
        deploy(args.host, args.restart, args.key)


if __name__ == "__main__":
    main()
