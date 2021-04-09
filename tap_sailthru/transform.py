import datetime
from email import utils

import singer


def email_datestring_to_datetime(datestring: str) -> datetime:
    """
    Takes in a date string in RFC 2822 format and parses it into datetime.

    :param datestring: the date string in RFC 2822 format
    :return: datetime object in UTC timezone
    """
    dt = utils.parsedate_to_datetime(datestring).isoformat()
    return singer.utils.strptime_to_utc(dt)


def _format_date_for_job_params(dt: datetime) -> str:
    """
    Takes in a datetime and returns a string padded with 0 in YYYYMMDD format.

    :param dt: a datetime object
    :return: a string in the form of 'YYYYMMDD'
    """
    if dt.day < 10:
        day = f'{dt.day:02d}'
    else:
        day = f'{dt.day}'

    if dt.month < 10:
        month = f'{dt.month:02d}'
    else:
        month = f'{dt.month}'

    return f'{dt.year}{month}{day}'


def get_start_and_end_date_params(start_date: str) -> str:
    """
    Gets the 'start_date' from the config and returns a string in the form
    of 'YYYYMMMDD'.

    :param start_date: the start_date datestring from the config file
    :return: date string in the form 'YYYYMMDD'
    """

    start = singer.utils.strptime_to_utc(start_date)
    end = start + datetime.timedelta(days=30)

    return _format_date_for_job_params(start), _format_date_for_job_params(end)
