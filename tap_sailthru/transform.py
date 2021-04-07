from datetime import datetime
from email import utils

import singer


def email_datestring_to_datetime(datestring: str) -> datetime:
    """Takes in a date string in RFC 2822 format and parses it into datetime.

    :param datestring: the date string in RFC 2822 format
    :return: datetime object in UTC timezone
    """
    dt = utils.parsedate_to_datetime(datestring).isoformat()
    return singer.utils.strptime_to_utc(dt)
