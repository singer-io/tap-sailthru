import csv
import time

import requests
import singer

from tap_sailthru.transform import (flatten_user_response,
                                    get_start_and_end_date_params,
                                    rfc2822_to_datetime, sort_by_rfc2822)

LOGGER = singer.get_logger()

class BaseStream:
    object_type = None
    tap_stream_id = None
    replication_method = None
    replication_key = None
    key_properties = []
    valid_replication_keys = []
    params = {}
    parent = None

    def __init__(self, client):
        self.client = client

    def get_records(self, options=None):
        raise NotImplementedError("Child classes of BaseStream require `get_records` implementation")

    def set_parameters(self, params):
        self.params = params


    def get_data_for_children(self):
        raise NotImplementedError("Implementation required for streams with children")

    def get_parent_data(self):
        parent = self.parent(self.client)
        return parent.get_data_for_children()

    def post_job(self, parameter=None):
        job_name = self.params.get('job')
        if parameter:
            LOGGER.info(f'Starting background job for {job_name}, parameter={parameter}')
        else:
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
            record_replication_value = rfc2822_to_datetime(record[self.replication_key])
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

        options = {
            'config': config,
        }

        for record in self.get_records(options=options):
            transformed_record = transformer.transform(record, stream_schema, stream_metadata)
            singer.write_record(self.tap_stream_id, transformed_record)

        singer.write_state(state)
        return state


class AdTargeterPlans(FullTableStream):
    tap_stream_id = 'ad_targeter_plans'
    key_properties = ['plan_id']

    def get_records(self, options=None):
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

    def get_records(self, options=None):
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

    def get_records(self, options=None):

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

    def get_records(self, options=None):
        response = self.client.get_blast_repeats().get_body()
        repeats = response['repeats']
        # Sort repeats by 'modify_time' field
        sorted_repeats = sort_by_rfc2822(repeats, sort_key='modify_time')

        yield from sorted_repeats

class Lists(FullTableStream):
    tap_stream_id = 'lists'
    key_properties = ['list_id']

    def get_records(self, options=None):
        response = self.client.get_lists().get_body()
        yield from response['lists']

    def get_data_for_children(self):
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

    def get_data_for_children(self):

        return self.get_records()

    def get_records(self, options=None):

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

    def get_records(self, options=None):

        if options:
            config = options.get('config')
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
