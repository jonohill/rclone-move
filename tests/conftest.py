import sys
from types import ModuleType

# plex_refresh is fetched into the image at build time, so it isn't in the repo
plex_refresh = ModuleType("plex_refresh")
plex_refresh.scan_paths = lambda paths: []  # type: ignore[attr-defined]
sys.modules["plex_refresh"] = plex_refresh
