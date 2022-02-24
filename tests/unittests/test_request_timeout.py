import unittest
import requests
from tap_sailthru.client import SailthruClient
from unittest import mock

# Mock response object
def get_mock_http_response(*args, **kwargs):
    contents = '{"access_token": "test", "expires_in":100, "accounts":[{"id": 12}]}'
    response = requests.Response()
    response.status_code = 200
    response._content = contents.encode()
    return response

@mock.patch('requests.Session.request', side_effect = get_mock_http_response)
class TestRequestTimeoutValue(unittest.TestCase):

    def test_no_request_timeout_in_config(self, mocked_request):
        """
            Verify that if request_timeout is not provided in config then default value is used
        """
        client = SailthruClient("test", "test", "test", None) # No request_timeout in config

        # Call _make_request method which call Session.request with timeout
        client._make_request("http://test", "test", "test")

        # Verify session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), 300) # Verify timeout argument

    def test_integer_request_timeout_in_config(self, mocked_request):
        """
            Verify that if request_timeout is provided in config(integer value) then it should be used.
        """
        client = SailthruClient("test", "test", "test", 100) # integer timeout in config

        # Call _make_request method which call Session.request with timeout
        client._make_request("http://test", "test", "test")

        # Verify session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), 100.0) # Verify timeout argument

    def test_float_request_timeout_in_config(self, mocked_request):
        """
            Verify that if request_timeout is provided in config(float value) then it should be used.
        """
        client = SailthruClient("test", "test", "test", 100.5)

        # Call _make_request method which call Session.request with timeout
        client._make_request("http://test", "test", "test")

        # Verify session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), 100.5) # Verify timeout argument

    def test_string_request_timeout_in_config(self, mocked_request):
        """
            Verify that if request_timeout is provided in config(string value) then it should be used.
        """
        client = SailthruClient("test", "test", "test", "100") # string format timeout in config

        # Call _make_request method which call Session.request with timeout
        client._make_request("http://test", "test", "test")

        # Verify session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), 100) # Verify timeout argument

    def test_empty_string_request_timeout_in_config(self, mocked_request):
        """
            Verify that if request_timeout is provided in the config with an empty string then the default value got used.
        """
        client = SailthruClient("test", "test", "test", "") # empty string in config

        # Call _make_request method which call Session.request with timeout
        client._make_request("http://test", "test", "test")

        # Verify session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), 300) # Verify timeout argument

    def test_zero_request_timeout_in_config(self, mocked_request):
        """
            Verify that if request_timeout is provided in the config with zero value then the default value got used.
        """
        client = SailthruClient("test", "test", "test", 0.0) # zero value in config

        # Call _make_request method which call Session.request with timeout
        client._make_request("http://test", "test", "test")

        # Verify session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), 300) # Verify timeout argument

    def test_zero_string_request_timeout_in_config(self, mocked_request):
        """
            Verify that if request_timeout is provided in the config with zero in string format then the default value got used.
        """
        client = SailthruClient("test", "test", "test", '0.0') # zero value in config

        # Call _make_request method which call Session.request with timeout
        client._make_request("http://test", "test", "test")

        # Verify session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), 300) # Verify timeout argument


@mock.patch("time.sleep")
class TestRequestTimeoutBackoff(unittest.TestCase):

    @mock.patch('requests.Session.send', side_effect = requests.exceptions.Timeout)
    def test_request_timeout_backoff(self, mocked_request, mocked_sleep):
        """
            Verify request function is backing off 3 times on the Timeout exception.
        """
        client = SailthruClient("test", "test", "test", 300)

        with self.assertRaises(requests.exceptions.Timeout):
            client._make_request("http://test", "test", "test")

        # Verify that Session.send is called 3 times
        self.assertEqual(mocked_request.call_count, 3)
