"""
Module for transformation and utility functions.
"""

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
    datetime_obj = utils.parsedate_to_datetime(datestring).isoformat()
    return singer.utils.strptime_to_utc(datetime_obj)


def get_start_and_end_date_params(start_datetime: datetime) -> datetime:
    """
    Returns the start datetime and an end datetime that is 30 days added
    to the start datetime.

    :param start_datetime: A datetime object
    :return: A 'start' datetime object and a 'end' datetime object that
        is 30 days from the start datetime
    """
    return start_datetime, start_datetime + datetime.timedelta(days=30)


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
        'profile_id': response.get('keys', {}).get('sid'),
        'cookie': response.get('keys', {}).get('cookie'),
        'email': response.get('keys', {}).get('email'),
        'vars': response.get('vars'),
        'lists': list(response.get('lists', {}).keys()),
        'engagement': response.get('engagement'),
        'optout_email': response.get('optout_email'),
    }


def advance_date_by_microsecond(date: str) -> str:
    """
    Adds a microsecond to the date.

    :param date: The date string to add a microsecond to
    :return: A new date string with an added microsecond
    """

    new_dt = singer.utils.strptime_to_utc(date) + datetime.timedelta(microseconds=1)

    return singer.utils.strftime(new_dt)


def get_purchase_key_type(record: dict) -> str:
    """
    Get's the purchase key type field name for a purchase log record.

    :param record: A dictionary containing a purchase log record
    :return: The purchase key type
    """
    if record.get('Extid') is not None:
        return 'Extid'

    return 'Sid'
