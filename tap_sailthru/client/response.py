# -*- coding: utf-8 -*-
"""
This module wraps additional functionality for a Sailthru API response object.
"""

try:
    import simplejson as json
except ImportError:
    import json

# pylint: disable=missing-class-docstring
class SailthruResponse:
    def __init__(self, response):
        self.response = response
        self.json_error = None

        try:
            self.json = json.loads(response.content)
        except ValueError as error:
            self.json = None
            self.json_error = str(error)

    # pylint: disable=missing-function-docstring
    def is_ok(self):
        return self.json and not set(["error", "errormsg"]) == set(self.json)

    # pylint: disable=missing-function-docstring
    def get_body(self, as_dictionary=True):
        if as_dictionary:
            return self.json

        return self.response.content

    # pylint: disable=missing-function-docstring
    def get_response(self):
        return self.response

    # pylint: disable=missing-function-docstring
    def get_status_code(self):
        return self.response.status_code

    # pylint: disable=missing-function-docstring
    def get_error(self):
        if self.is_ok():
            return False

        if self.json_error is None:
            code = self.json["error"]
            msg = self.json["errormsg"]
        else:
            code = 0
            msg = self.json_error

        return SailthruResponseError(msg, code)

    # pylint: disable=missing-function-docstring
    def get_rate_limit_headers(self):
        headers = self.response.headers

        if all(k in headers for
               k in ('X-Rate-Limit-Limit',
                     'X-Rate-Limit-Remaining',
                     'X-Rate-Limit-Reset')):
            return { 'limit' : int(headers['X-Rate-Limit-Limit']),
                     'remaining' : int(headers['X-Rate-Limit-Remaining']),
                     'reset' : int(headers['X-Rate-Limit-Reset']) }

        return None

# pylint: disable=missing-class-docstring
class SailthruResponseError:
    def __init__(self, message, code):
        self.message = message
        self.code = code

    # pylint: disable=missing-function-docstring
    def get_message(self):
        return self.message

    # pylint: disable=missing-function-docstring
    def get_error_code(self):
        return self.code
