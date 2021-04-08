from abc import abstractmethod
import csv
import time

import requests
import singer

from tap_sailthru.transform import email_datestring_to_datetime

LOGGER = singer.get_logger()

class BaseStream:
    object_type = None
    tap_stream_id = None
    replication_method = None
    replication_key = None
    key_properties = []
    valid_replication_keys = []
    params = {}

    def __init__(self, client):
        self.client = client

    def get_records(self, options=None):
        raise NotImplementedError("Child classes of BaseStream require `get_records` implementation")

    def set_parameters(self, params):
        self.params = params

    def post_job(self):
        job_name = self.params.get('job')
        LOGGER.info(f'Starting background job for {job_name}')
        # TODO: handle non 200 responses
        return self.client.create_job(self.params).get_body()

    def get_job_url(self, job_id):
        status = ''
        while status != 'completed':
            response = self.client.get_job(job_id).get_body()
            status = response.get('status')
            LOGGER.info(f'Job report status: {status}')
            time.sleep(1)

        return response.get('export_url')

    def process_job_csv(self, export_url, chunk_size=1024):
        """
        Fetches csv from URL and yields each line.
        """
        with requests.get(export_url, stream=True) as r:
            reader = csv.DictReader(line.decode('utf-8') for line in r.iter_lines(chunk_size=chunk_size))
            for row in reader:
                yield row


class IncrementalStream(BaseStream):
    replication_method = 'INCREMENTAL'

    def __init__(self, client):
        super().__init__(client)

    def sync(self, state, stream_schema, stream_metadata, config, transformer):
        start_time = singer.get_bookmark(state, self.tap_stream_id, self.replication_key, config['start_date'])
        max_record_value = start_time
        for record in self.get_records():
            record_replication_value = email_datestring_to_datetime(record[self.replication_key])
            if record_replication_value > singer.utils.strptime_to_utc(max_record_value):
                singer.write_record(
                    self.tap_stream_id,
                    record,
                )
                max_record_value = record_replication_value.isoformat()

        state = singer.write_bookmark(state, self.tap_stream_id, self.replication_key, max_record_value)
        singer.write_state(state)
        return state


class FullTableStream(BaseStream):
    replication_method = 'FULL_TABLE'

    def __init__(self, client):
        super().__init__(client)

    def sync(self, state, stream_schema, stream_metadata, config, transformer):

        for record in self.get_records():
            singer.write_record(self.tap_stream_id, record)

        singer.write_state(state)
        return state


class AdTargeterPlans(FullTableStream):
    tap_stream_id = 'ad_targeter_plans'
    key_properties = ['plan_id']

    def get_records(self):
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

    def get_records(self):
        for status in self.params['statuses']:
            # TODO: handle non 200 responses
            response = self.client.get_blasts(status).get_body()
            yield from response['blasts']


class BlastRecipients(FullTableStream):
    tap_stream_id = 'blast_recipients'
    key_properties = ['user_id']
    params = {
        'job': 'blast_query',
        'blast_id': '{blast_id}',
    }

    def get_records(self, options=None):
        # TODO: hardcoding for now
        params = {
            'job': 'blast_query',
            'blast_id': 23302084,
        }

        self.set_parameters(params)
        # TODO: hardcoding id for now
        response = self.post_job()
        export_url = self.get_job_url(job_id=response['job_id'])
        LOGGER.info(f'export_url: {export_url}')

        yield from self.process_job_csv(export_url=export_url)


class BlastRepeats(FullTableStream):
    tap_stream_id = 'blast_repeats'
    key_properties = ['repeat_id']


class Lists(FullTableStream):
    tap_stream_id = 'lists'
    key_properties = ['list_id']

    def get_records(self):
        response = self.client.get_lists().get_body()
        yield from response['lists']


class ListUsers(FullTableStream):
    tap_stream_id = 'list_users'
    key_properties = ['user_id']
    params = {
        'job': 'export_list_data',
        'list': '{list_name}',
    }

    def get_records(self, options=None):
        # TODO: hardcoding params for now
        params = {
            'job': 'export_list_data',
            'list': 'bytecode-employees-test',
        }

        self.set_parameters(params)
        # TODO: hardcoding id for now
        response = self.post_job()
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


class PurchaseLog(FullTableStream):
    tap_stream_id = 'purchase_log'
    key_properties = ['purchase_id']
    params = {
        'job': 'export_purchase_log',
        'start_date': '{purchase_log_start_date}',
        'end_date': '{purchase_log_end_date}',
    }

    def get_records(self, options=None):
        # TODO: hardcoding params for now
        params = {
            'job': 'export_purchase_log',
            'start_date': 20210310,
            'end_date': 20210408,
        }
        # TODO: hardcoding id for now
        self.set_parameters(params)
        response = self.post_job()
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


STREAMS = {
    'ad_targeter_plans': AdTargeterPlans,
    'blasts': Blasts,
    'blast_recipients': BlastRecipients,
    'blast_repeats': BlastRepeats,
    'lists': Lists,
    'list_users': ListUsers,
    'users': Users,
    'purchase_log': PurchaseLog,
    'purchases': Purchases,
}
