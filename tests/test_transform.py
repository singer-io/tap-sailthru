import datetime

import pytz

from tap_sailthru.transform import (_format_date_for_job_params,
                                    advance_date_by_microsecond,
                                    flatten_user_response,
                                    get_start_and_end_date_params,
                                    rfc2822_to_datetime, sort_by_rfc2822)


def test_rfc2822_to_datetime():
    datestring = 'Wed, 31 Mar 2021 22:15:07 -0400'
    expected = datetime.datetime(2021, 4, 1, 2, 15, 7, tzinfo=pytz.utc)

    result = rfc2822_to_datetime(datestring)

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


def test_sort_by_rfc2822():
    test_cases = [
        {'case': [
            {'modify_time': 'Mon, 19 Apr 2021 06:03:15 -0000', 'random_value': 61},
            {'modify_time': 'Wed, 07 Apr 2021 06:03:15 -0000', 'random_value': 11},
            {'modify_time': 'Sun, 04 Apr 2021 06:03:15 -0000', 'random_value': 42},
            {'modify_time': 'Thu, 15 Apr 2021 06:03:15 -0000', 'random_value': 81},
            {'modify_time': 'Thu, 08 Apr 2021 06:03:15 -0000', 'random_value': 4},
        ],
        'expected': [
            {'modify_time': 'Sun, 04 Apr 2021 06:03:15 -0000', 'random_value': 42},
            {'modify_time': 'Wed, 07 Apr 2021 06:03:15 -0000', 'random_value': 11},
            {'modify_time': 'Thu, 08 Apr 2021 06:03:15 -0000', 'random_value': 4},
            {'modify_time': 'Thu, 15 Apr 2021 06:03:15 -0000', 'random_value': 81},
            {'modify_time': 'Mon, 19 Apr 2021 06:03:15 -0000', 'random_value': 61},
        ],
        'sort_key': 'modify_time',
        }
    ]

    for test_case in test_cases:
        result = sort_by_rfc2822(data=test_case['case'], sort_key=test_case['sort_key'])

        assert test_case['expected'] == result


def test_flatten_user_response():
    test_cases = [
        {'case': {
            'keys': {
                'sid': 'pid1234',
                'cookie': 'cookie1234',
                'email': 'random.user@bytecode.io'
                },
            'vars': None,
            'lists': {
                'my-user-list': 'Wed, 24 Mar 2021 14:25:42 -0400'
                },
            'engagement': 'disengaged',
            'optout_email': 'none'},
        'expected': {
            'user_id': 'pid1234',
            'cookie': 'cookie1234',
            'email': 'random.user@bytecode.io',
            'vars': None,
            'lists': ['my-user-list'],
            'engagement': 'disengaged',
            'optout_email': 'none',
            }
        },
        {'case': {
            'keys': {
                'sid': None,
                'cookie': None,
                'email': None,
            },
            'vars': None,
            'lists': {},
            'engagement': None,
            'optout_email': None},
        'expected': {
            'user_id': None,
            'cookie': None,
            'email': None,
            'vars': None,
            'lists': [],
            'engagement': None,
            'optout_email': None,
            }
        },
    ]

    for test_case in test_cases:
        result = flatten_user_response(test_case['case'])

        assert test_case['expected'] == result


def test_advance_date_by_microsecond():
    test_cases = [
        {'case': '2021-04-06T19:42:08.000000Z', 'expected': '2021-04-06T19:42:08.000001Z'},
        {'case': '2000-01-01T00:00:00Z', 'expected': '2000-01-01T00:00:00.000001Z'},
    ]

    for test_case in test_cases:
        result = advance_date_by_microsecond(test_case['case'])

        assert test_case['expected'] == result
