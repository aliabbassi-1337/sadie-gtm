"""Deep-link service — public interface."""

from services.deeplink.service import (
    create_direct_link,
    create_proxy_session,
    create_short_link,
    get_proxy_session,
    resolve_short_link,
)

__all__ = [
    "create_direct_link",
    "create_proxy_session",
    "create_short_link",
    "resolve_short_link",
    "get_proxy_session",
]
