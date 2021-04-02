# streams: API URL endpoints to be called

STREAMS = {
    'ad_targeter_plans': {
        'key_properties': ['plan_id'],
        'replication_method': 'FULL_TABLE',
    },
    'blasts': {
        'key_properties': ['blast_id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['modify_time'],
        'params': {
            'statuses': ['sent', 'sending', 'unscheduled', 'scheduled']
        },
        'children': {
            'blast_recipients': {
                'key_properties': ['user_id'],
                'replication_method': 'FULL_TABLE',
                'params': {
                    'job': 'blast_query',
                    'blast_id': '{blast_id}'
                },
            },
        }
    },
    'blast_repeats': {
        'key_properties': ['repeat_id'],
        'replication_method': 'FULL_TABLE',
    },
    'lists': {
        'key_properties': ['list_id'],
        'replication_method': 'FULL_TABLE',
        'children': {
            'list_users': {
                'key_properties': ['user_id'],
                'replication_method': 'FULL_TABLE',
                'params': {
                    'job': 'export_list_data',
                    'list': '{list_name}',
                },
                'children': {
                    'users': {
                        'key_properties': ['user_id'],
                        'replication_method': 'FULL_TABLE',
                        'params': {
                            'id': '{user_id}',
                            'key': 'sid',
                        },
                    },
                }
            },
        }
    },
    'purchase_log': {
        'key_properties': ['purchase_id'],
        'replication_method': 'FULL_TABLE',
        'replication_keys': [],
        'params': {
            'job': 'export_purchase_log',
            'start_date': '{purchase_log_start_date}',
            'end_date': '{purchase_log_end_date}',
        },
        'children': {
            'purchases': {
                'key_properties': ['item_id'],
                'replication_method': 'INCREMENTAL',
                'replication_keys': ['time'],
                'params': {
                    'purchase_id': '{purchase_id}',
                    'purchase_key': 'sid',
                },
            },
        }
    },
}
