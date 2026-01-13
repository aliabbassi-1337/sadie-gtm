import asyncio
import sys
from db.client import init_db, close_db


async def main(workflow_name: str):
    """Main entry point for running workflows with DB initialization."""
    await init_db()
    try:
        if workflow_name == "scraper":
            from workflows.scraper import scraper_workflow
            await scraper_workflow()
        else:
            print(f"Unknown workflow: {workflow_name}")
            sys.exit(1)
    finally:
        await close_db()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python main.py <workflow_name>")
        sys.exit(1)

    workflow_name = sys.argv[1]
    asyncio.run(main(workflow_name))
