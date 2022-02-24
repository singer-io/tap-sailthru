"""
Python client for Sailthru API
"""

import hashlib
import json
import math
import sys
from typing import Union

import backoff
from requests import Session
from requests.exceptions import Timeout
from singer import get_logger, metrics

LOGGER = get_logger()

# Backoff retries
MAX_RETRIES = 3

# timeout request after 300 seconds
REQUEST_TIMEOUT = 300

# pylint: disable=missing-class-docstring
class SailthruClientError(Exception):
    pass

# pylint: disable=missing-class-docstring
class SailthruClientStatsNotReadyError(Exception):
    pass

# pylint: disable=missing-class-docstring
class SailthruClient429Error(Exception):
    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response

# pylint: disable=missing-class-docstring
class SailthruServer5xxError(Exception):
    pass

class SailthruBadRequestError(SailthruClientError):
    pass

class SailthruUnauthorizedError(SailthruClientError):
    pass

class SailthruForbiddenError(SailthruClientError):
    pass

class SailthruNotFoundError(SailthruClientError):
    pass

class SailthruConflictError(SailthruClientError):
    pass

class SailthruMethodNotFoundError(SailthruClientError):
    pass

class SailthruInternalServerError(SailthruServer5xxError):
    pass

# error code to exception class and error message mapping
ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": SailthruBadRequestError,
        "message": "The request is missing or has a bad parameter."
    },
    401: {
        "raise_exception": SailthruUnauthorizedError,
        "message": "Invalid authorization credentials."
    },
    403: {
        "raise_exception": SailthruForbiddenError,
        "message": "User does not have permission to access the resource."
    },
    404: {
        "raise_exception": SailthruNotFoundError,
        "message": "The resource you have specified cannot be found."
    },
    405: {
        "raise_exception": SailthruMethodNotFoundError,
        "message": "The provided HTTP method is not supported by the URL."
    },
    409: {
        "raise_exception": SailthruConflictError,
        "message": "The request could not be completed due to a conflict with the current state of the server."
    },
    429: {
        "raise_exception": SailthruClient429Error,
        "message": "API rate limit exceeded, please retry after some time."
    },
    500: {
        "raise_exception": SailthruInternalServerError,
        "message": "An error has occurred at Sailthru's end."
    }
}

# get exception class based on status code
def get_exception_for_status_code(status_code, error_code):
    # return SailthruServer5xxError if status code is greater than 500
    if status_code > 500:
        return SailthruServer5xxError
    if status_code == 400 and error_code == 99:
        return SailthruClientStatsNotReadyError

    return ERROR_CODE_EXCEPTION_MAPPING.get(status_code, {}).get("raise_exception", SailthruClientError)

# raise error with proper message based in error code from the response
def raise_for_error(response):
    status_code = response.status_code
    try:
        json_response = response.json()
    except Exception:
        json_response = {}

    # get sailthru error code, message and prepare message
    error_code = json_response.get("error")
    error_message = json_response.get("errormsg", ERROR_CODE_EXCEPTION_MAPPING.get(status_code, {}).get("message", "Unknown Error"))
    message = "HTTP-error-code: {}, Error: {}, Message: {}".format(status_code, error_code, error_message)

    # get exception class
    exception = get_exception_for_status_code(status_code, error_code)

    # return without raising error as for "blast_query" we have to skip syncing for that blast id
    if status_code == 403 and error_code == 99:
        LOGGER.warning("{}".format(json_response))
        return

    # add response with exception for 429 error, for 'retry_after_wait_gen'
    if status_code == 429:
        raise exception(message, response) from None
    raise exception(message) from None

def retry_after_wait_gen():
    while True:
        # This is called in an except block so we can retrieve the exception
        # and check it.
        exc_info = sys.exc_info()
        resp = exc_info[1].response
        sleep_time_str = resp.headers.get('X-Rate-Limit-Remaining')
        LOGGER.info(f'API rate limit exceeded -- sleeping for '
                    f'{sleep_time_str} seconds')
        yield math.floor(float(sleep_time_str))


class SailthruClient:
    base_url = 'https://api.sailthru.com'

    def __init__(self, api_key, api_secret, user_agent, request_timeout=REQUEST_TIMEOUT) -> None:
        self.__api_key = api_key
        self.__api_secret = api_secret
        self.session = Session()
        self.headers = {'User-Agent': user_agent}

        # Set request timeout to config param `request_timeout` value.
        # If value is 0,"0","" or not passed then it set default to 300 seconds.
        if request_timeout and float(request_timeout):
            self.__request_timeout = float(request_timeout)
        else:
            self.__request_timeout = REQUEST_TIMEOUT 

    def check_platform_access(self) -> None:
        """
        Check that provided credentials are valid or not by requesting sample settings.
        """
        self.get('/settings', None)

    def extract_params(self, params: Union[list, dict]) -> list:
        """
        Extracts the values of a set of parameters, recursing into nested dictionaries.

        :param params: dictionary values to generate signature string
        :return: A list of values
        """
        values = []
        if isinstance(params, dict):
            for value in params.values():
                values.extend(self.extract_params(value))
        elif isinstance(params, list):
            for value in params:
                values.extend(self.extract_params(value))
        else:
            values.append(params)
        return values

    def get_signature_string(self,
                             params: Union[list, dict],
                             secret: str) -> bytes:
        """
        Returns the unhashed signature string
        (secret + sorted list of param values) for an API call.

        :param params: dictionary values to generate signature string
        :param secret: secret string
        :return: A bytes object
        """
        str_list = [str(item) for item in self.extract_params(params)]
        str_list.sort()
        return (secret + ''.join(str_list)).encode('utf-8')

    def get_signature_hash(self, params: Union[list, dict], secret: str) -> str:
        """
        Returns an MD5 hash of the signature string for an API call.

        :param params: dictionary values to generate signature hash
        :param sercret: secret string
        :return: A hashed string
        """
        return hashlib.md5(self.get_signature_string(params, secret)).hexdigest()

    def get_lists(self, params: dict = None) -> dict:
        """
        Get all the lists in Sailthru.

        Docs: https://getstarted.sailthru.com/developers/api/list/

        :param params: Dict containig params to be passed to the API
        :return: A dict containing the API response.
        """
        return self.get('/list', params)

    def get_ad_targeter_plans(self, params: dict = None) -> dict:
        """
        Get all info on Ad Targeter Plans.

        Docs: https://getstarted.sailthru.com/developers/api/ad-plan/

        :param params: Dict containig params to be passed to the API
        :return: A dict containing the API response.
        """
        return self.get('/ad/plan', params)

    def get_blasts(self, params: dict = None) -> dict:
        """
        Get information about campaigns (blasts).

        Docs: https://getstarted.sailthru.com/developers/api/blast/
        Endpoint does not have ability to return all blasts. Can only
        query by blast status.

        :param params: Dict containig params to be passed to the API
        :return: A dict containing the API response.
        """
        if not params.get('status') and not params.get('blast_id'):
            raise SailthruClientError('Endpoint requires either "blast_id"'
                                      'or "status" parameter')

        return self.get('/blast', params)

    def get_blast_repeats(self, params: dict = None) -> dict:
        """
        Get all the recurring mass mail campaigns.

        Docs: https://getstarted.sailthru.com/developers/api/blast_repeat/

        :param params: Dict containig params to be passed to the API
        :return: A dict containing the API response.
        """
        return self.get('/blast_repeat', params)

    def get_user(self, params: dict = None) -> dict:
        """
        Get user profile data.

        Docs: https://getstarted.sailthru.com/developers/api/user/

        :param params: Dict containig params to be passed to the API
        :return: A dict containing the API response.
        """
        if not params.get('id'):
            raise SailthruClientError('Required "id" parameter missing')

        return self.get('/user', params)

    def get_job(self, params: dict = None) -> dict:
        """
        Get status and export URL for job.

        Docs: https://getstarted.sailthru.com/developers/api/job

        :param params: Dict containig params to be passed to the API
        :return: A dict containing the API response.
        """
        if not params.get('job_id'):
            raise SailthruClientError('Required "job_id" parameter missing')

        return self.get('/job', params)

    def create_job(self, params: dict = None) -> dict:
        """
        Create data export job.

        Docs: https://getstarted.sailthru.com/developers/api/job

        :param params: Dict containig params to be passed to the API
        :return: A dict containing the API response.
        """
        if not params.get('job'):
            raise SailthruClientError('Required "job" type parameter missing')

        return self.post('/job', params)

    # pylint: disable=missing-function-docstring
    def get(self, endpoint, params):
        return self._build_request(endpoint, params, 'GET')

    # pylint: disable=missing-function-docstring
    def post(self, endpoint, params):
        return self._build_request(endpoint, params, 'POST')

    def _build_request(self, endpoint, params, method):
        url = f"{self.base_url}{endpoint}"
        payload = self._prepare_payload(params)
        return self._make_request(url, payload, method)


    @backoff.on_exception(retry_after_wait_gen,
                          SailthruClient429Error,
                          max_tries=MAX_RETRIES)
    @backoff.on_exception(backoff.expo,
                          (SailthruClientError,
                          SailthruServer5xxError,
                          SailthruClientStatsNotReadyError,
                          Timeout),
                          max_tries=MAX_RETRIES,
                          factor=2)
    def _make_request(self, url, payload, method):

        params, data = (None, payload) if method == 'POST' else (payload, None)

        with metrics.http_request_timer(url) as timer:
            response = self.session.request(method=method,
                                            url=url,
                                            params=params,
                                            data=data,
                                            headers=self.headers,
                                            timeout=self.__request_timeout)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        # raise error if status code is not 200
        if response.status_code != 200:
            raise_for_error(response)

        return response.json()

    def _prepare_payload(self, data):
        payload = {
            'api_key': self.__api_key,
            'format': 'json',
            'json': json.dumps(data)
        }
        signature = self.get_signature_hash(payload, self.__api_secret)
        payload['sig'] = signature
        return payload
