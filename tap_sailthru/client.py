# client class: api_key, api_secret, user_agent
# http error codes

import hashlib
import json
from typing import Union
from requests import Session

# pylint: disable=missing-class-docstring
class SailthruClientError(Exception):
    pass

# pylint: disable=missing-class-docstring
class SailthruClient429Error(Exception):
    pass

# pylint: disable=missing-class-docstring
class SailthruServer5xxError(Exception):
    pass

# pylint: disable=missing-class-docstring
class SailthruJobTimeoutError(Exception):
    pass


class SailthruClient:
    base_url = 'https://api.sailthru.com'

    def __init__(self, api_key, api_secret, user_agent) -> None:
        self.__api_key = api_key
        self.__api_secret = api_secret
        self.session = Session()
        self.headers = {'User-Agent': user_agent}

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

    def get_signature_string(self, params: Union[list, dict], secret: str) -> bytes:
        """
        Returns the unhashed signature string (secret + sorted list of param values) for an API call.

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

    def get_lists(self) -> dict:
        """
        Queries the /list endpoint to get all the lists in Sailthru.

        Docs: https://getstarted.sailthru.com/developers/api/list/

        :return: A dict containing the API response.
        """
        return self.get('/list', None)

    def get(self, endpoint, data):
        return self._build_request(endpoint, data, 'GET')

    def post(self, endpoint, data):
        return self._build_request(endpoint, data, 'GET')

    def _build_request(self, endpoint, data, method):
        url = f"{self.base_url}/{endpoint}"
        payload = self._prepare_payload(data)
        return self._make_request(url, payload, method)

    def _make_request(self, url, payload, method):
        response = self.session.request(method=method,
                                        url=url,
                                        params=payload,
                                        headers=self.headers)
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
