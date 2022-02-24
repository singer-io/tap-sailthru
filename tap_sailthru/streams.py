"""
This module defines the stream classes and their individual sync logic.
"""

import csv
import datetime
import time
from datetime import date, timedelta
from functools import lru_cache
from typing import Any, Iterator

import requests
import singer
from singer import Transformer, metrics

from tap_sailthru.client import SailthruClient, SailthruClientError
from tap_sailthru.transform import (flatten_user_response,
                                    get_start_and_end_date_params,
                                    rfc2822_to_datetime,
                                    transform_keys_to_snake_case)

LOGGER = singer.get_logger()

# pylint: disable=missing-class-docstring
class SailthruJobTimeoutError(Exception):
    pass

class BaseStream:
    """
    A base class representing singer streams.

    :param client: The API client used extract records from the external source
    """
    object_type = None
    tap_stream_id = None
    replication_method = None
    replication_key = None
    key_properties = []
    valid_replication_keys = []
    params = {}
    parent = None
    date_keys = []

    def __init__(self, client: SailthruClient):
        self.client = client

    def get_records(self, bookmark_datetime: datetime = None, is_parent: bool = False) -> list:
        """
        Returns a list of records for that stream.

        :param bookmark_datetime: The datetime object representing the
            bookmark date
        :param is_parent: If true, may change the type of data
            that is returned for a child stream to consume
        :return: list of records
        """
        raise NotImplementedError("Child classes of BaseStream require "
                                  "`get_records` implementation")

    def get_parent_data(self, bookmark_datetime: datetime = None) -> list:
        """
        Returns a list of records from the parent stream.

        :param bookmark_datetime: The datetime object representing the
            bookmark date
        :return: A list of records
        """
        # pylint: disable=not-callable
        parent = self.parent(self.client)
        return parent.get_records(bookmark_datetime, is_parent=True)

    def post_job(self, parameter: Any = None) -> dict:
        """
        Creates a data export background job. More details:
        https://getstarted.sailthru.com/developers/api/job/

        :param parameter: Any parameter type to be passed to the logger
        :return: The API response as a dictionary
        """
        job_name = self.params.get('job')
        if parameter:
            # pylint: disable=logging-fstring-interpolation
            LOGGER.info(f'Starting background job for {job_name},'
                        f'parameter={parameter}')
        else:
            # pylint: disable=logging-fstring-interpolation
            LOGGER.info(f'Starting background job for {job_name}')
        return self.client.create_job(self.params)

    def get_job_url(self, job_id: str, timeout: int = 600) -> str:
        """
        Polls the /job endpoint and checks to see if export job is completed.
        Returns the export URL when job is ready.

        :param job_id: the job_id to poll
        :param timeout: the default timeout (seconds) before halting request
        :return: the export URL
        """
        status = ''
        job_start_time = singer.utils.now()
        while status != 'completed':
            response = self.client.get_job({'job_id': job_id})
            status = response.get('status')
            # pylint: disable=logging-fstring-interpolation
            LOGGER.info(f'Job report status: {status}')
            now = singer.utils.now()
            if (now - job_start_time).seconds > timeout:
                # pylint: disable=logging-fstring-interpolation
                LOGGER.critical(f'Request with job_id {job_id}'
                                f' exceeded {timeout} second timeout'
                                f'latest_status: {status}')
                raise SailthruJobTimeoutError
            time.sleep(1)

        return response.get('export_url')

    @staticmethod
    def process_job_csv(export_url: str,
                        chunk_size: int = 1024,
                        parent_params: dict = None) -> Iterator[dict]:
        """
        Fetches CSV from URL and streams each line.

        :param export_url: The URL from which to fetch the CSV data from
        :param chunk_size: The chunk size to read per line
        :param parent_params: A dictionary with "parent" parameters to append
            to each record
        :return: A generator of a dictionary
        """
        with requests.get(export_url, stream=True) as req:
            reader = csv.DictReader(line.decode('utf-8') for line
                                    in req.iter_lines(chunk_size=chunk_size))
            for row in reader:
                if parent_params:
                    row.update(parent_params)
                yield row

    def date_records_to_datetime(self, record):
        for key in self.date_keys:
            if key in record:
                if record[key]:
                    record[key] = rfc2822_to_datetime(record[key]).isoformat()


# pylint: disable=abstract-method
class IncrementalStream(BaseStream):
    """
    A child class of a base stream used to represent streams that use the
    INCREMENTAL replication method.

    :param client: The API client used extract records from the external source
    """
    replication_method = 'INCREMENTAL'

    # pylint: disable=too-many-arguments
    def sync(self,
             state: dict,
             stream_schema: dict,
             stream_metadata: dict,
             config: dict,
             transformer: Transformer) -> dict:
        """
        The sync logic for an incremental stream.

        :param state: A dictionary representing singer state
        :param stream_schema: A dictionary containing the stream schema
        :param stream_metadata: A dictionnary containing stream metadata
        :param config: A dictionary containing tap config data
        :param transformer: A singer Transformer object
        :return: State data in the form of a dictionary
        """
        start_date = singer.get_bookmark(state,
                                         self.tap_stream_id,
                                         self.replication_key,
                                         config['start_date'])

        bookmark_datetime = singer.utils.strptime_to_utc(start_date)
        max_datetime = bookmark_datetime

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(bookmark_datetime):
                self.date_records_to_datetime(record)
                transform_keys_to_snake_case(record)
                record_datetime = singer.utils.strptime_to_utc(record[self.replication_key])
                if record_datetime >= bookmark_datetime:
                    transformed_record = transformer.transform(record,
                                                               stream_schema,
                                                               stream_metadata)
                    singer.write_record(self.tap_stream_id, transformed_record)
                    counter.increment()
                    max_datetime = max(record_datetime, max_datetime)
            bookmark_date = singer.utils.strftime(max_datetime)

        state = singer.write_bookmark(state,
                                      self.tap_stream_id,
                                      self.replication_key,
                                      bookmark_date)
        return state

# pylint: disable=abstract-method
class FullTableStream(BaseStream):
    """
    A child class of a base stream used to represent streams that use the
    FULL_TABLE replication method.

    :param client: The API client used extract records from the external source
    """
    replication_method = 'FULL_TABLE'

    # pylint: disable=too-many-arguments
    def sync(self,
             state: dict,
             stream_schema: dict,
             stream_metadata: dict,
             config: dict,
             transformer: Transformer) -> dict:
        """
        The sync logic for an full table stream.

        :param state: A dictionary representing singer state
        :param stream_schema: A dictionary containing the stream schema
        :param stream_metadata: A dictionnary containing stream metadata
        :param config: A dictionary containing tap config data
        :param transformer: A singer Transformer object
        :return: State data in the form of a dictionary
        """
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                transform_keys_to_snake_case(record)
                transformed_record = transformer.transform(record, stream_schema, stream_metadata)
                singer.write_record(self.tap_stream_id, transformed_record)
                counter.increment()

        return state


class AdTargeterPlans(FullTableStream):
    """
    Gets records for Sailthru Ad Targeter Plans.

    Docs: https://getstarted.sailthru.com/developers/api/ad-plan/
    """
    tap_stream_id = 'ad_targeter_plans'
    key_properties = ['plan_id']

    def get_records(self, bookmark_datetime=None, is_parent=False):
        response = self.client.get_ad_targeter_plans()
        if not response.get('ad_plans'):
            LOGGER.critical('response is empty for ad_plans')
            raise SailthruClientError
        yield from response['ad_plans']


class Blasts(IncrementalStream):
    """
    Gets records for blasts (aka Sailtrhu campaigns).

    Docs: https://getstarted.sailthru.com/developers/api/blast/

    This endpoint does not have a method of retrieving all blasts available.
    You can only retrieve blast information by specifying a blast_id or you
    can get all blasts by status type (sent, sending, scheduled, unscheduled).
    """
    tap_stream_id = 'blasts'
    key_properties = ['blast_id']
    replication_key = 'modify_time'
    valid_replication_keys = ['modify_time']
    params = {
        'statuses': ['sent', 'sending', 'unscheduled', 'scheduled'],
    }
    date_keys = ['start_time', 'modify_time', 'schedule_time']

    def get_records(self, bookmark_datetime=None, is_parent=False):
        # Will just return a list of blast_id if being called
        # by child stream
        if is_parent:
            for status in self.params['statuses']:
                response = self.client.get_blasts({'status': status})
                yield from (blast.get('blast_id') for blast in response.get('blasts'))
        else:
            for status in self.params['statuses']:
                response = self.client.get_blasts({'status': status})
                # Add the blast status to each blast record
                yield from (dict(item, status=status) for item in response.get('blasts'))


class BlastQuery(FullTableStream):
    """
    Triggers asynchronous data export job for blasts and then fetches the
    records when the job is done.

    Docs: https://getstarted.sailthru.com/developers/api/job/#blast-query
    """
    tap_stream_id = 'blast_query'
    key_properties = ['profile_id', 'blast_id']
    params = {
        'job': 'blast_query',
        'blast_id': '{blast_id}',
    }
    parent = Blasts
    date_keys = ['send_time',
                 'open_time',
                 'click_time',
                 'purchase_time',
                 'first_ten_clicks_time']

    def get_records(self, bookmark_datetime=None, is_parent=False):

        for blast_id in self.get_parent_data():
            self.params['blast_id'] = blast_id

            response = self.post_job(parameter=blast_id)
            if response.get("error"):
                # https://getstarted.sailthru.com/developers/api/job/#Error_Codes
                # Error code 99 = You may not export a blast that has been sent
                # pylint: disable=logging-fstring-interpolation
                LOGGER.info(f"Skipping blast_id: {blast_id}")
                continue
            export_url = self.get_job_url(job_id=response['job_id'])

            # Add blast id to each record
            yield from self.process_job_csv(export_url=export_url,
                                            parent_params={'blast_id': blast_id})


class BlastRepeats(IncrementalStream):
    """
    Gets all recurring campaigns.

    Docs: https://getstarted.sailthru.com/developers/api/blast_repeat/
    """
    tap_stream_id = 'blast_repeats'
    key_properties = ['repeat_id']
    replication_key = 'modify_time'
    valid_replication_keys = ['modify_time']
    date_keys = ['create_time',
                 'modify_time',
                 'start_date',
                 'end_date',
                 'error_time']

    def get_records(self, bookmark_datetime=None, is_parent=False):
        response = self.client.get_blast_repeats()
        if not response.get('repeats'):
            LOGGER.critical("response is empty for blast_repeats")
            raise SailthruClientError
        yield from response.get('repeats')


class Lists(FullTableStream):
    """
    Retrieves info for Sailthru lists.

    Docs: https://getstarted.sailthru.com/developers/api/list/
    """
    tap_stream_id = 'lists'
    key_properties = ['list_id']
    date_keys = ['create_time']

    # pylint: disable=missing-function-docstring
    @lru_cache
    def get_lists(self):
        return self.client.get_lists()

    def get_records(self, bookmark_datetime=None, is_parent=False):

        response = self.get_lists()
        if not response.get('lists'):
            LOGGER.critical("response is empty for lists")
            raise SailthruClientError

        # Will just return list names if called by child stream
        if is_parent:
            for record in response['lists']:
                yield record['name']
        else:
            yield from response['lists']


class BlastSaveList(FullTableStream):
    """
    Triggers asynchronous data export job for blast_save_query and then
    fetches the records when the job is done.

    Docs: https://getstarted.sailthru.com/developers/api/job/#blast-save-list
    """
    tap_stream_id = 'blast_save_list'
    key_properties = ['profile_id']
    params = {
        'job': 'export_list_data',
        'list': '{list_name}',
    }
    parent = Lists
    date_keys = ['profile_created_date',
                 'optout_time',
                 'first_purchase_time',
                 'last_purchase_time',
                 'profile_created_date']

    def get_records(self, bookmark_datetime=None, is_parent=False):

        for list_name in self.get_parent_data():
            self.params['list'] = list_name

            response = self.post_job(parameter=self.params['list'])
            export_url = self.get_job_url(job_id=response['job_id'])
            yield from self.process_job_csv(export_url=export_url)

class Users(FullTableStream):
    """
    Retrieve user profile data.

    Docs: https://getstarted.sailthru.com/developers/api/user/
    """
    tap_stream_id = 'users'
    key_properties = ['profile_id']
    params = {
        'id': '{profile_id}',
        'key': 'sid',
    }
    parent = BlastSaveList

    def get_records(self,
                    bookmark_datetime: datetime = None,
                    is_parent: bool = None):

        for record in self.get_parent_data():
            if not record.get('Profile Id'):
                LOGGER.critical('no Profile Id for record')
                continue
            profile_id = record['Profile Id']
            response = self.client.get_user({'id': profile_id})
            yield flatten_user_response(response)


class PurchaseLog(IncrementalStream):
    """
    Triggers asynchronous data export job for export_purchase_log and then
    fetches the records when the job is done.

    Docs: https://getstarted.sailthru.com/developers/api/job/#export-purchase-log
    """
    tap_stream_id = 'purchase_log'
    key_properties = ['date', 'email_hash', 'extid', 'message_id', 'price', 'channel']
    replication_key = 'date'
    valid_replication_keys = ['date']
    params = {
        'job': 'export_purchase_log',
        'start_date': '{purchase_log_start_date}',
        'end_date': '{purchase_log_end_date}',
    }
    date_keys = ['date']

    def get_records(self, bookmark_datetime=None, is_parent=False):

        start_datetime, end_datetime = get_start_and_end_date_params(bookmark_datetime)
        now = singer.utils.now()

        # Generate a report for each day up until the today's date
        while start_datetime.date() <= now.date():

            job_date = start_datetime.strftime('%Y%m%d')
            self.params['start_date'] = job_date
            self.params['end_date'] = job_date

            response = self.post_job(parameter=(self.params['start_date'],
                                     self.params['end_date']))
            export_url = self.get_job_url(job_id=response['job_id'])
            yield from self.process_job_csv(export_url=export_url)

            start_datetime += timedelta(days=1)


STREAMS = {
    'ad_targeter_plans': AdTargeterPlans,
    'blasts': Blasts,
    'blast_query': BlastQuery,
    'blast_repeats': BlastRepeats,
    'lists': Lists,
    'blast_save_list': BlastSaveList,
    'users': Users,
    'purchase_log': PurchaseLog,
}
