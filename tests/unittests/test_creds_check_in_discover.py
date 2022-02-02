import unittest
import requests
import json
import tap_sailthru
from tap_sailthru.discover import discover
from unittest import mock

# Mock response object
def get_mock_http_response(status_code, content={}):
    contents = json.dumps(content)
    response = requests.Response()
    response.status_code = status_code
    response.headers = {}
    response._content = contents.encode()
    return response

@mock.patch('requests.Session.request')
@mock.patch('tap_sailthru.discover.get_schemas', side_effect = tap_sailthru.discover.get_schemas)
class TestCredCheckInDiscoverMode(unittest.TestCase):

    def test_invalid_creds_401(self, mocked_schema, mocked_request):
        """
            Verify exception is raised for no access(401) error code for auth
            and get_schemas() is not called due to exception.
        """
        # Prepare config and mock request to raise 401 error code
        config = {
            "start_date": "2020-04-01T00:00:00Z",
            "user_agent": "Stitch Tap (+support@stitchdata.com)",
            "api_key": "test",
            "api_secret": "test",
        }
        mocked_request.return_value = get_mock_http_response(401, {})

        # Verify SailthruClientError exception is raised with 401 status code
        with self.assertRaises(tap_sailthru.client.SailthruClientError) as e:
            catalog = discover(config)
            self.assertEqual(e.response.status_code, 401)

        # Verify that get_schemas() is not called due to invalid credentials
        self.assertEqual(mocked_schema.call_count, 0)
        
    def test_valid_creds_200(self, mocked_schema, mocked_request):
        """
            Verify get_schemas() is called if auth credentials are valid
            and catalog object is returned from discover().
        """
        # Prepare config and mock request to return 200 status code
        config = {
            "start_date": "2020-04-01T00:00:00Z",
            "user_agent": "Stitch Tap (+support@stitchdata.com)",
            "api_key": "test",
            "api_secret": "test",
        }
        mocked_request.return_value = get_mock_http_response(200, {})

        # Call discover mode
        catalog = discover(config)

        # Verify that get_schemas() called once and catalog object is returned from discover()
        self.assertEqual(mocked_schema.call_count, 1)
        self.assertTrue(isinstance(catalog, tap_sailthru.discover.Catalog))