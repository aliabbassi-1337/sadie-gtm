"""FastAPI application for the deep-link service.

Run:
    uv run uvicorn api.deeplink.app:app --reload --port 8000
"""

import logging

logging.basicConfig(level=logging.INFO)

from fastapi import FastAPI

from api.deeplink.proxy import catchall_router, router as proxy_router
from api.deeplink.routes import router as api_router

app = FastAPI(title="Sadie Deep-Link Service")

# Mount specific proxy routes (before app routes is fine — they have specific prefixes)
app.include_router(proxy_router)

# API routes: POST /api/deeplink, GET /r/{code}
app.include_router(api_router)

# Catch-all proxy — MUST be last so it doesn't shadow /api/deeplink or /r/{code}.
# Handles WAF challenges (Imperva /_Incapsula_Resource), static assets, etc.
app.include_router(catchall_router)
