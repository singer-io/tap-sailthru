import datetime
from email import utils

import singer

LOGGER = singer.get_logger()

def email_datestring_to_datetime(datestring):
    dt = utils.parsedate_to_datetime(datestring).isoformat()
    return singer.utils.strptime_to_utc(dt)

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

    def get_records(self):
        raise NotImplementedError("Child classes of BaseStream require `get_records` implementation")


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
            response = self.client.get_blasts(status).get_body()
            yield from response['blasts']


class BlastRecipients(FullTableStream):
    tap_stream_id = 'blast_recipients'
    key_properties = ['user_id']
    params = {
        'job': 'blast_query',
        'blast_id': '{blast_id}',
    }


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
