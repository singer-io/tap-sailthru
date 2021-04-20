import csv
import time
from typing import Any, Generator

import requests
import singer

from tap_sailthru.client import SailthruClient
from tap_sailthru.transform import (flatten_user_response,
                                    get_start_and_end_date_params,
                                    rfc2822_to_datetime, sort_by_rfc2822)

LOGGER = singer.get_logger()

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

    def __init__(self, client: SailthruClient):
        self.client = client

    def get_records(self, config: dict = None) -> list:
        """
        Returns a list of records for that stream.

        :param config: The tap config file
        :return: list of records
        """
        raise NotImplementedError("Child classes of BaseStream require `get_records` implementation")

    def set_parameters(self, params: dict) -> None:
        """
        Sets or updates the `params` attribute of a class.

        :param params: Dictionary of parameters to set or update the class with
        """
        self.params = params

    def get_data_for_children(self, config: dict = None) -> list:
        """
        Returns a list of records to be consumed by child streams.

        :param config: The tap config file
        :return: A list of records
        """
        raise NotImplementedError("Implementation required for streams with children")

    def get_parent_data(self, config: dict = None) -> list:
        """
        Returns a list of records from the parent stream.

        :param config: The tap config file
        :return: A list of records
        """
        parent = self.parent(self.client)
        return parent.get_data_for_children(config)

    def post_job(self, parameter: Any = None) -> dict:
        """
        Creates a data export background job. More details:
        https://getstarted.sailthru.com/developers/api/job/

        :param parameter: Any parameter type to be passed to the logger
        :return: The API response as a dictionary
        """
        job_name = self.params.get('job')
        if parameter:
            LOGGER.info(f'Starting background job for {job_name}, parameter={parameter}')
        else:
            LOGGER.info(f'Starting background job for {job_name}')
        # TODO: handle non 200 responses
        return self.client.create_job(self.params).get_body()

    def get_job_url(self, job_id: str) -> str:
        """
        Polls the /job endpoint and checks to see if export job is completed.
        Returns the export URL when job is ready.

        :param job_id: the job_id to poll
        :return: the export URL
        """
        status = ''
        while status != 'completed':
            response = self.client.get_job(job_id).get_body()
            status = response.get('status')
            LOGGER.info(f'Job report status: {status}')
            time.sleep(1)

        return response.get('export_url')

    @staticmethod
    def process_job_csv(export_url: str, chunk_size: int = 1024) -> Generator[dict]:
        """
        Fetches CSV from URL and streams each line.

        :param export_url: The URL from which to fetch the CSV data from
        :param chunk_size: The chunk size to read per line
        :return: A generator of a dictionary
        """
        with requests.get(export_url, stream=True) as r:
            reader = csv.DictReader(line.decode('utf-8') for line in r.iter_lines(chunk_size=chunk_size))
            for row in reader:
                yield row


class IncrementalStream(BaseStream):
    replication_method = 'INCREMENTAL'
    batched = False

    def __init__(self, client):
        super().__init__(client)

    def sync(self, state, stream_schema, stream_metadata, config, transformer):
        start_time = singer.get_bookmark(state, self.tap_stream_id, self.replication_key, config['start_date'])
        max_record_value = start_time
        for record in self.get_records(config):
            record_replication_value = rfc2822_to_datetime(record[self.replication_key])
            if self.batched:
                if record_replication_value >= singer.utils.strptime_to_utc(max_record_value):
                    singer.write_record(
                        self.tap_stream_id,
                        record,
                    )
            else:
                if record_replication_value > singer.utils.strptime_to_utc(max_record_value):
                    singer.write_record(
                        self.tap_stream_id,
                        record,
                    )
                # TODO: ensure results are ordered
                max_record_value = record_replication_value.isoformat()

        state = singer.write_bookmark(state, self.tap_stream_id, self.replication_key, max_record_value)
        singer.write_state(state)
        return state


class FullTableStream(BaseStream):
    replication_method = 'FULL_TABLE'

    def __init__(self, client):
        super().__init__(client)

    def sync(self, state, stream_schema, stream_metadata, config, transformer):

        for record in self.get_records(config):
            transformed_record = transformer.transform(record, stream_schema, stream_metadata)
            singer.write_record(self.tap_stream_id, transformed_record)

        singer.write_state(state)
        return state


class AdTargeterPlans(FullTableStream):
    tap_stream_id = 'ad_targeter_plans'
    key_properties = ['plan_id']

    def get_records(self, config=None):
        response = self.client.get_ad_targeter_plans().get_body()
        yield from response['ad_plans']


class Blasts(IncrementalStream):
    tap_stream_id = 'blasts'
    key_properties = ['blast_id']
    replication_key = 'modify_time'
    valid_replication_keys = ['modify_time']
    params = {
        'statuses': ['sent', 'sending', 'unscheduled', 'scheduled'],
    }

    def get_records(self, config=None):
        for status in self.params['statuses']:
            response = self.client.get_blasts(status).get_body()
            yield from response['blasts']

    def get_data_for_children(self):
        blast_ids = []
        for status in self.params['statuses']:
            response = self.client.get_blasts(status).get_body()
            ids = [blast.get('blast_id') for blast in response['blasts']]
            blast_ids.extend(ids)

        yield from blast_ids


class BlastQuery(FullTableStream):
    tap_stream_id = 'blast_query'
    key_properties = ['profile_id']
    params = {
        'job': 'blast_query',
        'blast_id': '{blast_id}',
    }
    parent = Blasts

    def get_records(self, config=None):

        for blast_id in self.get_parent_data():
            params = {
                'job': 'blast_query',
                'blast_id': blast_id,
            }

            self.set_parameters(params)
            response = self.post_job(parameter=params['blast_id'])
            try:
                export_url = self.get_job_url(job_id=response['job_id'])
            except Exception as e:
                if response.get("error"):
                    # https://getstarted.sailthru.com/developers/api/job/#Error_Codes
                    # Error code 99 = You may not export a blast that has been sent
                    if response.get("error") == 99:
                        LOGGER.warn(f"{response.get('errormsg')}")
                        LOGGER.info(f"Skipping blast_id: {params['blast_id']}")
                        continue
                    else:
                        LOGGER.exception(e)
                        raise e
            LOGGER.info(f'export_url: {export_url}')

            yield from self.process_job_csv(export_url=export_url)


class BlastRepeats(IncrementalStream):
    tap_stream_id = 'blast_repeats'
    key_properties = ['repeat_id']
    replication_key = 'modify_time'
    valid_replication_keys = ['modify_time']

    def get_records(self, config=None):
        response = self.client.get_blast_repeats().get_body()
        repeats = response['repeats']
        # Sort repeats by 'modify_time' field
        sorted_repeats = sort_by_rfc2822(repeats, sort_key='modify_time')

        yield from sorted_repeats

class Lists(FullTableStream):
    tap_stream_id = 'lists'
    key_properties = ['list_id']

    def get_records(self, config=None):
        response = self.client.get_lists().get_body()
        yield from response['lists']

    def get_data_for_children(self, config=None):
        response = self.client.get_lists().get_body()
        for list in response['lists']:
            yield list['name']


class ListUsers(FullTableStream):
    tap_stream_id = 'list_users'
    key_properties = ['user_id']
    params = {
        'job': 'export_list_data',
        'list': '{list_name}',
    }
    parent = Lists

    def get_data_for_children(self, config=None):

        return self.get_records()

    def get_records(self, config=None):

        for list_name in self.get_parent_data():
            params = {
                'job': 'export_list_data',
                'list': list_name,
            }

            self.set_parameters(params)
            response = self.post_job(parameter=params['list'])
            export_url = self.get_job_url(job_id=response['job_id'])
            LOGGER.info(f'export_url: {export_url}')

            yield from self.process_job_csv(export_url=export_url)


class Users(FullTableStream):
    tap_stream_id = 'users'
    key_properties = ['user_id']
    params = {
        'id': '{user_id}',
        'key': 'sid',
    }
    parent = ListUsers

    def get_records(self, options):

        for record in self.get_parent_data():
            user_id = record['Profile Id']
            response = self.client.get_user(user_id).get_body()
            yield flatten_user_response(response)


class PurchaseLog(FullTableStream):
    tap_stream_id = 'purchase_log'
    key_properties = ['purchase_id']
    params = {
        'job': 'export_purchase_log',
        'start_date': '{purchase_log_start_date}',
        'end_date': '{purchase_log_end_date}',
    }

    def get_data_for_children(self, config):
        return self.get_records(config)

    def get_records(self, config=None):

        datestring = config.get('start_date')
        start_date, end_date = get_start_and_end_date_params(datestring)

        params = {
            'job': 'export_purchase_log',
            'start_date': start_date,
            'end_date': end_date,
        }

        self.set_parameters(params)
        response = self.post_job(parameter=(params['start_date'], params['end_date']))
        export_url = self.get_job_url(job_id=response['job_id'])
        LOGGER.info(f'export_url: {export_url}')

        yield from self.process_job_csv(export_url=export_url)


class Purchases(IncrementalStream):
    tap_stream_id = 'purchases'
    key_properties = ['item_id']
    replication_key = 'time'
    valid_replication_keys = ['time']
    params = {
        'purchase_id': '{purchase_id}',
        'purchase_key': 'sid',
    }
    parent = PurchaseLog
    batched = True

    def get_records(self, config=None):

        for record in self.get_parent_data(config):
            purchase_id = record.get("Extid")
            # TODO: figure out what to do if extid doesn't exist
            if not purchase_id:
                continue
            response = self.client.get_purchase(purchase_id, purchase_key='extid').get_body()

            if response.get("error"):
                LOGGER.info(f"record: {record}")
                LOGGER.info(f"purchase_id: {purchase_id}")
                LOGGER.info(f"response: {response}")

            yield response

STREAMS = {
    'ad_targeter_plans': AdTargeterPlans,
    'blasts': Blasts,
    'blast_query': BlastQuery,
    'blast_repeats': BlastRepeats,
    'lists': Lists,
    'list_users': ListUsers,
    'users': Users,
    'purchase_log': PurchaseLog,
    'purchases': Purchases,
}
