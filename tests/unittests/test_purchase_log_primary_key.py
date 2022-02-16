from tap_sailthru.client import SailthruClient
from tap_sailthru.streams import PurchaseLog
import unittest

class TestPurchaseLogPrimaryKey(unittest.TestCase):

    def test_purchase_log_primary_key(self):
        """
            Test case to verify the Primary Key for 'purchase_log' stream
        """

        # get client
        client = SailthruClient("test_api_key", "test_api_secret", "test_user_agent")
        # create purchase log object
        purchase_log = PurchaseLog(client=client)

        # expected PK for assertion
        expected_PK = ["date", "email_hash", "extid", "message_id", "price", "channel"]
        # actual PK from the purchase log object
        actual_PK = purchase_log.key_properties

        # verify the primary keys matches
        self.assertEqual(expected_PK, actual_PK)
