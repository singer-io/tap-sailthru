def config():
    return {
        "test_name": "test_sync_non_report_streams",
        "tap_name": "tap-sailthru",
        "type": "platform.sailthru",
        "properties": {
            "user_agent": "TAP_SAILTHRU_USER_AGENT",
            "start_date": "TAP_SAILTHRU_START_DATE"
        },
        "credentials": {
            "api_key": "TAP_SAILTHRU_API_KEY",
            "api_secret": "TAP_SAILTHRU_API_SECRET",
        },
        "bookmark": {
            "bookmark_key": "blasts",
            "bookmark_timestamp": "2021-04-01T15:10:32.000000Z"
        },
        "streams": {
            "ad_tageter_plans": {"plan_id"},
            "blast_query": {"profile_id"},
            "blast_repeats": {"repeat_id"},
            "blast_save_list": {"profile_id"},
            "blasts": {"blast_id"},
            "lists": {"list_id"},
            "purchase_log": {"extid"},
            "users": {"profile_id"},
        },
    }
