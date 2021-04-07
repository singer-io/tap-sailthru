import datetime

import pytz

from tap_sailthru.transform import email_datestring_to_datetime


def test_email_datestring_to_datetime():
    datestring = 'Wed, 31 Mar 2021 22:15:07 -0400'
    expected = datetime.datetime(2021, 4, 1, 2, 15, 7, tzinfo=pytz.utc)

    result = email_datestring_to_datetime(datestring)

    assert expected == result

