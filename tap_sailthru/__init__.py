"""
Entrypoint for tap_sailthru.
"""

import singer
from singer import utils

from tap_sailthru.discover import discover as _discover
from tap_sailthru.sync import sync

REQUIRED_CONFIG_KEYS = ["start_date", "api_key", "api_secret", "user_agent"]
LOGGER = singer.get_logger()


@utils.handle_top_exception(LOGGER)
def main():
    """
    Entrypoint function for tap.
    """
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = _discover(args.config)
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = _discover(args.config)
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
