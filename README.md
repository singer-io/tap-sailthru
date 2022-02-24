# tap-sailthru

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls data from the [Sailthru API](https://getstarted.sailthru.com/developers/api-basics/introduction/)
- Extracts from the following sources to produce [streams](tap_sailthru/streams.py). Below is a list of all the streams available. See the streams file for a list of classes where each one has a constant indiciating if the stream's replication_method is `INCREMENTAL` or `FULL_TABLE` and what the replication_key is, usually `modify_time` field if it's incremental.
  - [Ad Targeter Plan](https://getstarted.sailthru.com/developers/api/ad-plan/)
  - [Blast](https://getstarted.sailthru.com/developers/api/blast/)
  - [Blast Repeat](https://getstarted.sailthru.com/developers/api/blast_repeat/)
  - [Blast Query](https://getstarted.sailthru.com/developers/api/job/#blast-query)
  - [Blast Save List](https://getstarted.sailthru.com/developers/api/job/#blast-save-list)
  - [Export Purchase Log](https://getstarted.sailthru.com/developers/api/job/#export-purchase-log)
  - [List](https://getstarted.sailthru.com/developers/api/list/)
  - [User](https://getstarted.sailthru.com/developers/api/user/)

- Includes a schema for each resource reflecting most recent tested data retrieved using the api. See the [schemas](tap_sailthru/schemas) folder for details.

- Some streams incrementally pull data based on the previously saved state. See the [bookmarking strategy](README.md#bookmarking-strategy) section for more details.

## Bookmarking Strategy

Some endpoints in the Sailthru API support a `modify_time` (or similar) field that allows for `INCREMENTAL` replication. However, in most cases there is no such field available so most of the endpoints require `FULL_TABLE` replication. Furthermore, the API does not support pagination or filtering or ordering of any kind, so that should be taken into consideration for streams with a `FULL_TABLE` replication.

## Authentication

API key/secret can be retrieved by logging into your SailThru account then going to Settings > Setup > API & Postbacks > click lock icon to reveal credentials.

## Quick Start

1. Install

Clone this repository, and then install using setup.py. We recommend using a virtualenv:

```bash
$ virtualenv -p python3 venv
$ source venv/bin/activate
$ pip install -e .
```

2. Create your tap's config.json file. The tap config file for this tap should include these entries:

    - `start_date` - (rfc3339 date string) the default value to use if no bookmark exists for an endpoint
    - `user_agent` (string, required): Process and email for API logging purposes. Example: tap-sailthru <api_user_email@your_company.com>
    - `api_key` (string, required): The API key
    - `api_secret` (string, required): The API secret
    - `request_timeout` (string/integer/float, optional): The time for which request should wait to get response and default request_timeout is 300 seconds.

And the other values mentioned in the authentication section above.

```json
{
	"start_date": "2021-04-01T00:00:00Z",
	"user_agent": "Stitch Tap (+support@stitchdata.com)",
	"api_key": "<api_key>",
	"api_secret": "<api_secret>",
  "request_timeout": 300
}
```

Run the Tap in Discovery Mode This creates a catalog.json for selecting objects/fields to integrate:

```bash
tap-sailthru --config config.json --discover > catalog.json
```

See the Singer docs on discovery mode [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

4. Run the Tap in Sync Mode (with catalog) and write out to state file

For Sync mode:

```bash
$ tap-sailthru --config tap_config.json --catalog catalog.json >> state.json
$ tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
```

To load to json files to verify outputs:

```bash
$ tap-sailthru --config tap_config.json --catalog catalog.json | target-json >> state.json
$ tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
```

To pseudo-load to Stitch Import API with dry run:

```bash
$ tap-sailthru --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run >> state.json
$ tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
```

---

Copyright &copy; 2018 Stitch
