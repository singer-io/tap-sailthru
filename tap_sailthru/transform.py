import datetime
from email import utils
from typing import List

import singer


def rfc2822_to_datetime(datestring: str) -> datetime:
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


def sort_by_rfc2822(data: List[dict], sort_key) -> List[dict]:
    """
    Takes in a list of dictionaries and sorts them in ascending order by date.

    :param data: the list of dictionaries to sort
    :param sort_key: the name of the field containing an RFC 2822 formatted date
    :return: a list of dictionaries sorted in ascending order by their RFC 2822 date
    """
    return sorted(data, key=lambda row: rfc2822_to_datetime(row[sort_key]))


def flatten_user_response(response: dict) -> dict:
    """
    Takes in a response from the sailthru /user endpoint and flattens the response.

    :param response: the dictionary representing the response object from the api call
    :return: a flattened dicitonary
    """

    return {
        'user_id': response.get('keys', {}).get('sid'),
        'cookie': response.get('keys', {}).get('cookie'),
        'email': response.get('keys', {}).get('email'),
        'vars': response.get('vars'),
        'lists': [list_name for list_name in response.get('lists', {}).keys()],
        'engagement': response.get('engagement'),
        'optout_email': response.get('optout_email'),
    }
