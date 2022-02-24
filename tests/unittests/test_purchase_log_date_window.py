import datetime
import unittest
from unittest import mock
from tap_sailthru.streams import PurchaseLog
from tap_sailthru.client import SailthruClient

@mock.patch("tap_sailthru.streams.BaseStream.post_job")
@mock.patch("tap_sailthru.streams.BaseStream.get_job_url")
@mock.patch("tap_sailthru.streams.BaseStream.process_job_csv")
@mock.patch("singer.utils.now")
class PurchaseLogDateWindow(unittest.TestCase):
    """
        Test case to verify the date window for "PurchaseLog" stream
    """

    def test_date_window(self, mocked_now, mocked_process_job_csv, mocked_get_job_url, mocked_post_job):
        # mock singer.utils.now and return desired date
        mocked_now.return_value = datetime.datetime(2021, 3, 1, 4, 45)

        # set start date
        start_date = datetime.datetime(2021, 1, 1, 6, 30)

        # create SailthruClient object
        client = SailthruClient("test_api_key", "test_api_secret", "test_user_agent")
        # create PurchaseLog object
        purchase_log = PurchaseLog(client)

        # function call
        records = list(purchase_log.get_records(start_date))

        # verify the date window worked by comparing the call count of "post_job" function
        # start date = 01-01-2021, now date = 01-03-2021
        # date diff (in days) = 60 (inclusive), previously it was 30 calls
        self.assertEquals(mocked_post_job.call_count, 60)
