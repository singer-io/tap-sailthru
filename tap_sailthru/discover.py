"""
Module that handles the discovery logic for the tap.
"""

import json
import os
from typing import Tuple

from singer import metadata
from singer.catalog import Catalog

from tap_sailthru.streams import STREAMS
from tap_sailthru.client import SailthruClient


def _get_abs_path(path: str) -> str:
    """
    Gets the absolute path of a file.
    """
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def _get_key_properties_from_meta(schema_meta: list) -> str:
    """
    Gets the table-key-properties from the schema metadata.
    """
    return schema_meta[0].get('metadata').get('table-key-properties')

def _get_replication_method_from_meta(schema_meta: list) -> str:
    """
    Gets the forced-replication-method from the schema metadata.
    """
    return schema_meta[0].get('metadata').get('forced-replication-method')

def _get_replication_key_from_meta(schema_meta: list) -> str:
    """
    Gets the valid-replication-keys from the schema metadata.
    """
    if _get_replication_method_from_meta(schema_meta) == 'INCREMENTAL':
        return schema_meta[0].get('metadata').get('valid-replication-keys')[0]
    return None

def get_schemas() -> Tuple[dict, dict]:
    """
    Loads the schemas defined for the tap.

    This function iterates through the STREAMS dictionary which contains
    a mapping of the stream name and its corresponding class and loads
    the matching schema file from the schemas directory.
    """

    schemas = {}
    schemas_metadata = {}

    for stream_name, stream_object in STREAMS.items():

        schema_path = _get_abs_path('schemas/{}.json'.format(stream_name))
        with open(schema_path) as file:
            schema = json.load(file)

        meta = metadata.get_standard_metadata(
            schema=schema,
            key_properties=stream_object.key_properties,
            replication_method=stream_object.replication_method
        )

        meta = metadata.to_map(meta)

        if stream_object.valid_replication_keys:
            meta = metadata.write(meta,
                                  (),
                                  'valid-replication-keys',
                                  stream_object.valid_replication_keys)
        if stream_object.replication_key:
            meta = metadata.write(meta,
                                 ('properties', stream_object.replication_key),
                                 'inclusion', 'automatic')

        meta = metadata.to_list(meta)

        schemas[stream_name] = schema
        schemas_metadata[stream_name] = meta

    return schemas, schemas_metadata


def discover(config) -> Catalog:
    """
    Constructs a singer Catalog object based on the schemas and metadata.
    """

    # Initialize SailthruClient() and call check_platform_access() to verify credentials
    api_key, api_secret = config.get('api_key'), config.get('api_secret')
    client = SailthruClient(api_key, api_secret, config.get('user_agent'), config.get('request_timeout'))
    client.check_platform_access()

    schemas, schemas_metadata = get_schemas()
    streams = []

    for schema_name, schema in schemas.items():
        schema_meta = schemas_metadata[schema_name]

        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'key_properties': _get_key_properties_from_meta(schema_meta),
            'replication_method': _get_replication_method_from_meta(schema_meta),
            'replication_key': _get_replication_key_from_meta(schema_meta),
            'metadata': schema_meta
        }

        streams.append(catalog_entry)

    return Catalog.from_dict({'streams': streams})
