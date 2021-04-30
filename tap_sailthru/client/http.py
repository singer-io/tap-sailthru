# -*- coding: utf-8 -*-
"""
This module handles the http requests for the client.
"""

import platform

import backoff
import requests
from singer import metrics

from .error import SailthruClient429Error, SailthruClientError, SailthruServer5xxError
from .response import SailthruResponse


def flatten_nested_hash(hash_table):
    """
    Flatten nested dictionary for GET / POST / DELETE API request
    """
    def flatten(hash_table, brackets=True):
        field = {}
        for key, value in hash_table.items():
            _key = '[' + str(key) + ']' if brackets else str(key)
            if isinstance(value, dict):
                for key_inner, value_inner in flatten(value).items():
                    field[_key + key_inner] = value_inner
            elif isinstance(value, list):
                temp_hash = {}
                for i, value_inner in enumerate(value):
                    temp_hash[str(i)] = value_inner
                for key_inner, value_inner in flatten(temp_hash).items():
                    field[_key + key_inner] = value_inner
            else:
                field[_key] = value
        return field
    return flatten(hash_table, False)

# TODO: retry 500s, some client errors, 2 backoff decorators?
# pylint: disable=too-many-arguments
@backoff.on_exception(backoff.expo, (SailthruClient429Error), max_tries=2, factor=2)
def sailthru_http_request(url, data, method, user_agent, file_data=None, headers=None, request_timeout=10):
    """
    Perform an HTTP GET / POST / DELETE request
    """
    data = flatten_nested_hash(data)
    method = method.upper()
    params, data = (None, data) if method == 'POST' else (data, None)
    # TODO: use User-Agent from config; look for documentation for format
    # user_agent in config
    sailthru_headers = {'User-Agent': user_agent}
    if headers and isinstance(headers, dict):
        for key, value in sailthru_headers.items():
            headers[key] = value
    else:
        headers = sailthru_headers
    try:
        with metrics.http_request_timer(url) as timer:
            response = requests.request(method,
                                        url,
                                        params=params,
                                        data=data,
                                        files=file_data,
                                        headers=headers,
                                        timeout=request_timeout)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        if response.status_code == 429:
            raise SailthruClient429Error
        if response.status_code >= 500:
            raise SailthruServer5xxError

        return SailthruResponse(response)

    except requests.HTTPError as error:
        raise SailthruClientError(str(error)) from error
    except requests.RequestException as error:
        raise SailthruClientError(str(error)) from error
