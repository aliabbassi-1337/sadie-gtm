"""Deep-link service — public interface."""

from services.deeplink.service import (
    create_deeplink,
    create_deeplink_for_hotel,
    create_proxy_deeplink,
    create_short_link,
    get_proxy_session,
    resolve_short_link,
)

__all__ = [
    "create_deeplink",
    "create_proxy_deeplink",
    "create_short_link",
    "resolve_short_link",
    "get_proxy_session",
    "create_deeplink_for_hotel",
]
