"""Engine builder registry."""

from lib.deeplink.engines.cloudbeds import CloudbedsBuilder
from lib.deeplink.engines.mews import MewsBuilder
from lib.deeplink.engines.resnexus import ResNexusBuilder
from lib.deeplink.engines.rms import RmsBuilder
from lib.deeplink.engines.siteminder import SiteMinderBuilder

ENGINE_BUILDERS = {
    "SiteMinder": SiteMinderBuilder(),
    "Cloudbeds": CloudbedsBuilder(),
    "Mews": MewsBuilder(),
    "RMS Cloud": RmsBuilder(),
    "ResNexus": ResNexusBuilder(),
}

# Domain patterns for engine detection (reuses logic from lib/api_discovery/discoverer.py)
ENGINE_DOMAIN_PATTERNS = {
    "SiteMinder": ["direct-book.com"],
    "Cloudbeds": ["hotels.cloudbeds.com"],
    "Mews": ["app.mews.com"],
    "RMS Cloud": ["rmscloud.com"],
    "ResNexus": ["resnexus.com"],
}
