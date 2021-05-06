"""
Module for transformation and utility functions.
"""

import datetime
from email import utils

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


def flatten_user_response(response: dict) -> dict:
    """
    Takes in a response from the sailthru /user endpoint and flattens the response.

    :param response: the dictionary representing the response object from the api call
    :return: a flattened dicitonary
    """

    return {
        # TODO: should we keep date value from lists key? ask brian
        'profile_id': response.get('keys', {}).get('sid'),
        'cookie': response.get('keys', {}).get('cookie'),
        'email': response.get('keys', {}).get('email'),
        'vars': response.get('vars'),
        'lists': list(response.get('lists', {}).keys()),
        'engagement': response.get('engagement'),
        'optout_email': response.get('optout_email'),
    }


def _convert_to_snake_case(key: str) -> str:
    """
    Takes in a string and will convert it to snake case.

    :param key: The string to convert
    :return: A string converted to snake case
    """
    return '_'.join(key.split(' ')).lower()


def transform_keys_to_snake_case(record: dict) -> None:
    """
    Transforms all the keys of a dictionary to snake case.

    :param record: The dictionary to transform
    """
    for key in list(record.keys()):
        record[_convert_to_snake_case(key)] = record.pop(key)
