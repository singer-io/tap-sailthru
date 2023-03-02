import datetime

import pytz

from tap_sailthru.transform import (flatten_user_response,
                                    get_start_and_end_date_params,
                                    rfc2822_to_datetime)


def test_rfc2822_to_datetime():
    datestring = 'Wed, 31 Mar 2021 22:15:07 -0400'
    expected = datetime.datetime(2021, 4, 1, 2, 15, 7, tzinfo=pytz.utc)

    result = rfc2822_to_datetime(datestring)

    assert expected == result


def test_get_start_and_end_date_params():
    test_cases = [
        {'case': datetime.datetime(2021, 1, 1, 0, 0, tzinfo=pytz.utc), 'expected': (datetime.datetime(2021, 1, 1, 0, 0, tzinfo=pytz.utc), datetime.datetime(2021, 1, 31, 0, 0, tzinfo=pytz.utc))},
        {'case': datetime.datetime(1973, 12, 31, 0, 0, tzinfo=pytz.utc), 'expected': (datetime.datetime(1973, 12, 31, 0, 0, tzinfo=pytz.utc), datetime.datetime(1974, 1, 30, 0, 0, tzinfo=pytz.utc))},
        {'case': datetime.datetime(2002, 5, 11, 0, 0, tzinfo=pytz.utc), 'expected': (datetime.datetime(2002, 5, 11, 0, 0, tzinfo=pytz.utc), datetime.datetime(2002, 6, 10, 0, 0, tzinfo=pytz.utc))},
        {'case': datetime.datetime(1950, 10, 9, 0, 0, tzinfo=pytz.utc), 'expected': (datetime.datetime(1950, 10, 9, 0, 0, tzinfo=pytz.utc), datetime.datetime(1950, 11, 8, 0, 0, tzinfo=pytz.utc))},
    ]

    for test_case in test_cases:
        result = get_start_and_end_date_params(test_case['case'])

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
            'profile_id': 'pid1234',
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
            'profile_id': None,
            'cookie': None,
            'email': None,
            'vars': None,
            'lists': [],
            'engagement': None,
            'optout_email': None,
            }
        },
        {'case': {
            'keys': {
                'sid': None,
                'cookie': None,
                'email': None,
            },
            'vars': None,
            'lists': None,
            'engagement': None,
            'optout_email': None},
        'expected': {
            'profile_id': None,
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
