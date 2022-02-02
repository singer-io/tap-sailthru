from unittest import mock
import tap_sailthru.client as client
import unittest
import requests

# mocked response class
class Mockresponse:
    def __init__(self, status_code, json, raise_error, headers=None):
        self.status_code = status_code
        self.raise_error = raise_error
        self.text = json
        self.headers = headers

    def raise_for_status(self):
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("Sample message")

    def json(self):
        return self.text

# function to get mocked response
def get_response(status_code, json={}, raise_error=False, headers=None):
    return Mockresponse(status_code, json, raise_error, headers)

@mock.patch("requests.Session.request")
@mock.patch("time.sleep")
class TestExceptionHandling(unittest.TestCase):
    """
        Test cases to verify error is raised with proper message
    """

    def test_400_error_response_message(self, mocked_sleep, mocked_request):
        # mock json error response
        response_json = {"error": 9, "errormsg": "Bad request for the URL."}
        mocked_request.return_value = get_response(400, response_json, True)

        # create sailthru client
        sailthru_client = client.SailthruClient("test_api_key", "test_api_secret", "test_user_agent")
        with self.assertRaises(client.SailthruBadRequestError) as e:
            # function call
            sailthru_client._build_request("test_endpoint", {}, "GET")
            # verify the error is raised as expected with message
            self.assertEquals(str(e), "HTTP-error-code: 400, Error: 9, Message: Bad request for the URL.")

    def test_401_error_response_message(self, mocked_sleep, mocked_request):
        # mock json error response
        response_json = {"error": 9, "errormsg": "Unauthorized for the URL."}
        mocked_request.return_value = get_response(401, response_json, True)
        
        # create sailthru client
        sailthru_client = client.SailthruClient("test_api_key", "test_api_secret", "test_user_agent")
        with self.assertRaises(client.SailthruUnauthorizedError) as e:
            # function call
            sailthru_client._build_request("test_endpoint", {}, "GET")
            # verify the error is raised as expected with message
            self.assertEquals(str(e), "HTTP-error-code: 401, Error: 9, Message: Unauthorized for the URL.")

    def test_403_error_response_message(self, mocked_sleep, mocked_request):
        # mock json error response
        response_json = {"error": 9, "errormsg": "Forbidden for the URL."}
        mocked_request.return_value = get_response(403, response_json, True)
        
        # create sailthru client
        sailthru_client = client.SailthruClient("test_api_key", "test_api_secret", "test_user_agent")
        with self.assertRaises(client.SailthruForbiddenError) as e:
            # function call
            sailthru_client._build_request("test_endpoint", {}, "GET")
            # verify the error is raised as expected with message
            self.assertEquals(str(e), "HTTP-error-code: 403, Error: 9, Message: Forbidden for the URL.")

    def test_404_error_response_message(self, mocked_sleep, mocked_request):
        # mock json error response
        response_json = {"error": 9, "errormsg": "Not Found."}
        mocked_request.return_value = get_response(404, response_json, True)
        
        # create sailthru client
        sailthru_client = client.SailthruClient("test_api_key", "test_api_secret", "test_user_agent")
        with self.assertRaises(client.SailthruNotFoundError) as e:
            # function call
            sailthru_client._build_request("test_endpoint", {}, "GET")
            # verify the error is raised as expected with message
            self.assertEquals(str(e), "HTTP-error-code: 404, Error: 9, Message: Not Found.")

    def test_405_error_response_message(self, mocked_sleep, mocked_request):
        # mock json error response
        response_json = {"error": 9, "errormsg": "Method not found for the URL."}
        mocked_request.return_value = get_response(405, response_json, True)
        
        # create sailthru client
        sailthru_client = client.SailthruClient("test_api_key", "test_api_secret", "test_user_agent")
        with self.assertRaises(client.SailthruMethodNotFoundError) as e:
            # function call
            sailthru_client._build_request("test_endpoint", {}, "GET")
            # verify the error is raised as expected with message
            self.assertEquals(str(e), "HTTP-error-code: 405, Error: 9, Message: Bad request for the URL.")

    def test_409_error_response_message(self, mocked_sleep, mocked_request):
        # mock json error response
        response_json = {"error": 9, "errormsg": "Conflict occurred for the URL."}
        mocked_request.return_value = get_response(409, response_json, True)
        
        # create sailthru client
        sailthru_client = client.SailthruClient("test_api_key", "test_api_secret", "test_user_agent")
        with self.assertRaises(client.SailthruConflictError) as e:
            # function call
            sailthru_client._build_request("test_endpoint", {}, "GET")
            # verify the error is raised as expected with message
            self.assertEquals(str(e), "HTTP-error-code: 409, Error: 9, Message: Conflict occurred for the URL.")

    def test_429_error_response_message(self, mocked_sleep, mocked_request):
        # mock json error response
        response_json = {"error": 9, "errormsg": "Rate limit exceeded for the URL."}
        mocked_request.return_value = get_response(429, response_json, True, {"X-Rate-Limit-Remaining": 1})
        
        # create sailthru client
        sailthru_client = client.SailthruClient("test_api_key", "test_api_secret", "test_user_agent")
        with self.assertRaises(client.SailthruClient429Error) as e:
            # function call
            sailthru_client._build_request("test_endpoint", {}, "GET")
            # verify the error is raised as expected with message
            self.assertEquals(str(e), "HTTP-error-code: 429, Error: 9, Message: Rate limit exceeded for the URL.")

    def test_500_error_response_message(self, mocked_sleep, mocked_request):
        # mock json error response
        response_json = {"error": 9, "errormsg": "Internal server error occurred."}
        mocked_request.return_value = get_response(500, response_json, True)
        
        # create sailthru client
        sailthru_client = client.SailthruClient("test_api_key", "test_api_secret", "test_user_agent")
        with self.assertRaises(client.SailthruServer5xxError) as e:
            # function call
            sailthru_client._build_request("test_endpoint", {}, "GET")
            # verify the error is raised as expected with message
            self.assertEquals(str(e), "HTTP-error-code: 500, Error: 9, Message: Internal server error occurred.")

    def test_400_error_custom_message(self, mocked_sleep, mocked_request):
        # mock json error response
        response_json = {"error": 9, "message": "Bad request for the URL."}
        mocked_request.return_value = get_response(400, response_json, True)
        
        # create sailthru client
        sailthru_client = client.SailthruClient("test_api_key", "test_api_secret", "test_user_agent")
        with self.assertRaises(client.SailthruBadRequestError) as e:
            # function call
            sailthru_client._build_request("test_endpoint", {}, "GET")
            # verify the error is raised as expected with message
            self.assertEquals(str(e), "HTTP-error-code: 400, Error: 9, Message: The request is missing or has a bad parameter.")

    def test_401_error_custom_message(self, mocked_sleep, mocked_request):
        # mock json error response
        response_json = {"error": 9, "message": "Unauthorized for the URL."}
        mocked_request.return_value = get_response(401, response_json, True)
        
        # create sailthru client
        sailthru_client = client.SailthruClient("test_api_key", "test_api_secret", "test_user_agent")
        with self.assertRaises(client.SailthruUnauthorizedError) as e:
            # function call
            sailthru_client._build_request("test_endpoint", {}, "GET")
            # verify the error is raised as expected with message
            self.assertEquals(str(e), "HTTP-error-code: 401, Error: 9, Message: Invalid authorization credentials.")

    def test_403_error_custom_message(self, mocked_sleep, mocked_request):
        # mock json error response
        response_json = {"error": 9, "message": "Forbidden for the URL."}
        mocked_request.return_value = get_response(403, response_json, True)
        
        # create sailthru client
        sailthru_client = client.SailthruClient("test_api_key", "test_api_secret", "test_user_agent")
        with self.assertRaises(client.SailthruForbiddenError) as e:
            # function call
            sailthru_client._build_request("test_endpoint", {}, "GET")
            # verify the error is raised as expected with message
            self.assertEquals(str(e), "HTTP-error-code: 403, Error: 9, Message: User does not have permission to access the resource.")

    def test_404_error_custom_message(self, mocked_sleep, mocked_request):
        # mock json error response
        response_json = {"error": 9, "message": "Not Found."}
        mocked_request.return_value = get_response(404, response_json, True)
        
        # create sailthru client
        sailthru_client = client.SailthruClient("test_api_key", "test_api_secret", "test_user_agent")
        with self.assertRaises(client.SailthruNotFoundError) as e:
            # function call
            sailthru_client._build_request("test_endpoint", {}, "GET")
            # verify the error is raised as expected with message
            self.assertEquals(str(e), "HTTP-error-code: 404, Error: 9, Message: The resource you have specified cannot be found.")

    def test_405_error_custom_message(self, mocked_sleep, mocked_request):
        # mock json error response
        response_json = {"error": 9, "message": "Method not found for the URL."}
        mocked_request.return_value = get_response(405, response_json, True)
        
        # create sailthru client
        sailthru_client = client.SailthruClient("test_api_key", "test_api_secret", "test_user_agent")
        with self.assertRaises(client.SailthruMethodNotFoundError) as e:
            # function call
            sailthru_client._build_request("test_endpoint", {}, "GET")
            # verify the error is raised as expected with message
            self.assertEquals(str(e), "HTTP-error-code: 405, Error: 9, Message: The provided HTTP method is not supported by the URL.")

    def test_409_error_custom_message(self, mocked_sleep, mocked_request):
        # mock json error response
        response_json = {"error": 9, "message": "Conflict occurred for the URL."}
        mocked_request.return_value = get_response(409, response_json, True)
        
        # create sailthru client
        sailthru_client = client.SailthruClient("test_api_key", "test_api_secret", "test_user_agent")
        with self.assertRaises(client.SailthruConflictError) as e:
            # function call
            sailthru_client._build_request("test_endpoint", {}, "GET")
            # verify the error is raised as expected with message
            self.assertEquals(str(e), "HTTP-error-code: 409, Error: 9, Message: The request could not be completed due to a conflict with the current state of the server.")

    def test_429_error_custom_message(self, mocked_sleep, mocked_request):
        # mock json error response
        response_json = {"error": 9, "message": "Rate limit exceeded for the URL."}
        mocked_request.return_value = get_response(429, response_json, True, {"X-Rate-Limit-Remaining": 1})
        
        # create sailthru client
        sailthru_client = client.SailthruClient("test_api_key", "test_api_secret", "test_user_agent")
        with self.assertRaises(client.SailthruClient429Error) as e:
            # function call
            sailthru_client._build_request("test_endpoint", {}, "GET")
            # verify the error is raised as expected with message
            self.assertEquals(str(e), "HTTP-error-code: 429, Error: 9, Message: API rate limit exceeded, please retry after some time.")

    def test_500_error_custom_message(self, mocked_sleep, mocked_request):
        # mock json error response
        response_json = {"error": 9, "message": "Internal server error occurred."}
        mocked_request.return_value = get_response(500, response_json, True)
        
        # create sailthru client
        sailthru_client = client.SailthruClient("test_api_key", "test_api_secret", "test_user_agent")
        with self.assertRaises(client.SailthruServer5xxError) as e:
            # function call
            sailthru_client._build_request("test_endpoint", {}, "GET")
            # verify the error is raised as expected with message
            self.assertEquals(str(e), "HTTP-error-code: 500, Error: 9, Message: An error has occurred at Sailthru's end.")

    def test_200_response(self, mocked_sleep, mocked_request):
        # mock json error response
        response_json = {"key1": "value1", "key2": "value2"}
        mocked_request.return_value = get_response(200, response_json)
        
        # create sailthru client
        sailthru_client = client.SailthruClient("test_api_key", "test_api_secret", "test_user_agent")
        # function call
        response = sailthru_client._build_request("test_endpoint", {}, "GET")

        # verify the mocked data is coming as expected
        self.assertEquals(response, response_json)
