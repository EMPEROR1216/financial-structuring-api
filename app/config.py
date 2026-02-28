"""
Simple debug mode toggle for demo visibility.

Set DEBUG=1 or DEBUG=true to enable logs for upload, extraction, aggregation, CSV.
"""
import os

# Toggle via env: DEBUG=1 or DEBUG=true
DEBUG_MODE = os.getenv("DEBUG", "false").lower() in ("1", "true", "yes")


def debug_log(scope: str, message: str, *args) -> None:
    """Print when DEBUG_MODE is on. scope: UPLOAD, EXTRACTOR, AGGREGATOR, CSV."""
    if DEBUG_MODE:
        prefix = f"[{scope}]"
        if args:
            print(prefix, message, *args)
        else:
            print(prefix, message)
