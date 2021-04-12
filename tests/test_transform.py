import datetime

import pytz

from tap_sailthru.transform import (email_datestring_to_datetime,
                                    _format_date_for_job_params,
                                    get_start_and_end_date_params)


def test_email_datestring_to_datetime():
    datestring = 'Wed, 31 Mar 2021 22:15:07 -0400'
    expected = datetime.datetime(2021, 4, 1, 2, 15, 7, tzinfo=pytz.utc)

    result = email_datestring_to_datetime(datestring)

    assert expected == result


def test_format_date_for_job_params():
    test_cases = [
        {'case': datetime.datetime(2021, 1, 1), 'expected': '20210101'},
        {'case': datetime.datetime(1973, 12, 31), 'expected': '19731231'},
        {'case': datetime.datetime(2002, 5, 11), 'expected': '20020511'},
        {'case': datetime.datetime(1950, 10, 9), 'expected': '19501009'},
    ]

    for test_case in test_cases:
        result = _format_date_for_job_params(test_case['case'])

        assert test_case['expected'] == result


def test_get_start_and_end_date_params():
    test_cases = [
        {'case': '2021-01-01T00:00:00', 'expected': ('20210101', '20210131')},
        {'case': '1973-12-31T00:00:00', 'expected': ('19731231', '19740130')},
        {'case': '2002-05-11T00:00:00', 'expected': ('20020511', '20020610')},
        {'case': '1950-10-09T00:00:00', 'expected': ('19501009', '19501108')},
    ]

    for test_case in test_cases:
        result = get_start_and_end_date_params(test_case['case'])

        assert test_case['expected'] == result