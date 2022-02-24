import os
import unittest
import tap_tester.connections as connections
import tap_tester.menagerie as menagerie
import tap_tester.runner as runner
from datetime import datetime as dt
from datetime import timedelta

class SailthruBaseTest(unittest.TestCase):
    """
    Setup expectations for test sub classes
    Run discovery for as a prerequisite for most tests
    """
    AUTOMATIC_FIELDS = "automatic"
    REPLICATION_KEYS = "valid-replication-keys"
    PRIMARY_KEYS = "table-key-properties"
    FOREIGN_KEYS = "table-foreign-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    API_LIMIT = "max-row-limit"
    INCREMENTAL = "INCREMENTAL"
    FULL_TABLE = "FULL_TABLE"
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"
    OBEYS_START_DATE = "obey-start-date"

    @staticmethod
    def tap_name():
        """The name of the tap"""
        return "tap-sailthru"

    @staticmethod
    def get_type():
        """the expected url route ending"""
        return "platform.sailthru"

    def expected_streams(self):
        """A set of expected stream names"""
        return set(self.expected_metadata().keys())

    def untestable_streams(self):
        return set()

    def expected_sync_streams(self):
        return self.expected_streams() - self.untestable_streams()

    def expected_metadata(self):
        return {
            "ad_targeter_plans": {
                self.PRIMARY_KEYS : {"plan_id"},
                self.REPLICATION_METHOD : self.FULL_TABLE,
                self.OBEYS_START_DATE: False
            },
            "blasts": {
                self.PRIMARY_KEYS : {"blast_id"},
                self.REPLICATION_METHOD : self.INCREMENTAL,
                self.REPLICATION_KEYS : {"modify_time"},
                self.OBEYS_START_DATE: True
            },
            "blast_query": {
                self.PRIMARY_KEYS : {"profile_id", "blast_id"},
                self.REPLICATION_METHOD : self.FULL_TABLE,
                self.OBEYS_START_DATE: False
            },
            "blast_repeats": {
                self.PRIMARY_KEYS : {"repeat_id"},
                self.REPLICATION_METHOD : self.INCREMENTAL,
                self.REPLICATION_KEYS : {"modify_time"},
                self.OBEYS_START_DATE: True
            },
            "lists": {
                self.PRIMARY_KEYS : {"list_id"},
                self.REPLICATION_METHOD : self.FULL_TABLE,
                self.OBEYS_START_DATE: False
            },
            "blast_save_list": {
                self.PRIMARY_KEYS : {"profile_id"},
                self.REPLICATION_METHOD : self.FULL_TABLE,
                self.OBEYS_START_DATE: False
            },
            "users": {
                self.PRIMARY_KEYS : {"profile_id"},
                self.REPLICATION_METHOD : self.FULL_TABLE,
                self.OBEYS_START_DATE: False
            },
            "purchase_log": {
                self.PRIMARY_KEYS : {"extid"},
                self.REPLICATION_METHOD : self.INCREMENTAL,
                self.REPLICATION_KEYS : {"date"},
                self.OBEYS_START_DATE: True
            }
        }

    def run_and_verify_check_mode(self, conn_id):
        """
        Run the tap in check mode and verify it succeeds.
        This should be ran prior to field selection and initial sync.
        Return the connection id and found catalogs from menagerie.
        """

        # Run a check job using orchestrator (discovery)
        check_job_name = runner.run_check_mode(self, conn_id)

        # Assert that the check job succeeded
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['stream_name'], found_catalogs))
        self.assertSetEqual(self.expected_streams(), found_catalog_names, msg="discovered schemas do not match")
        print("discovered schemas are OK")

        return found_catalogs


    def get_properties(self, original_properties=True):
        properties = {
            'start_date': '2021-04-01T00:00:00Z'
        }

        if original_properties:
            return properties

        # Reassign start date
        properties["start_date"] = self.start_date
        return properties

    def get_credentials(self):
        return {
            "api_key": os.environ['TAP_SAILTHRU_API_KEY'],
            "api_secret": os.environ['TAP_SAILTHRU_API_SECRET']
        }

    def expected_primary_keys(self):
        """
        return a dictionary with key of table name and value as a set of primary key fields
        """
        return {table: properties.get(self.PRIMARY_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_replication_keys(self):
        """
        return a dictionary with key of table name and value as a set of replication key fields
        """
        return {table: properties.get(self.REPLICATION_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_automatic_fields(self):
        """
        return a dictionary with key of table name and value as a set of primary key and replication
        key fields
        """
        auto_fields = {}
        for k, v in self.expected_metadata().items():
            auto_fields[k] = v.get(self.PRIMARY_KEYS, set()) |  v.get(self.REPLICATION_KEYS, set())
        return auto_fields

    def expected_replication_method(self):
        """return a dictionary with key of table name and value of replication method"""
        return {table: properties.get(self.REPLICATION_METHOD, set())
                for table, properties
                in self.expected_metadata().items()}

    def select_all_streams_and_fields(self, conn_id, catalogs, select_all_fields: bool = True):
        """Select all streams and all fields within streams"""
        for catalog in catalogs:
            if catalog["tap_stream_id"] in self.expected_sync_streams():
                schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])

                non_selected_properties = []
                if not select_all_fields:
                    # get a list of all properties so that none are selected
                    non_selected_properties = schema.get('annotated-schema', {}).get(
                        'properties', {}).keys()

                connections.select_catalog_and_fields_via_metadata(
                    conn_id, catalog, schema, [], non_selected_properties)

    def run_and_verify_sync(self, conn_id):
        """
        Run a sync job and make sure it exited properly.
        Return a dictionary with keys of streams synced and values of records synced for each stream
        """
        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify actual rows were synced
        sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys())
        self.assertGreater(
            sum(sync_record_count.values()), 0,
            msg="failed to replicate any data: {}".format(sync_record_count)
        )
        print("total replicated row count: {}".format(sum(sync_record_count.values())))

        return sync_record_count

    def perform_and_verify_table_and_field_selection(self,
                                                     conn_id,
                                                     test_catalogs,
                                                     select_all_fields=True):
        """
        Perform table and field selection based off of the streams to select
        set and field selection parameters.
        Verify this results in the expected streams selected and all or no
        fields selected for those streams.
        """

        # Select all available fields or select no fields from all testable streams
        self.select_all_streams_and_fields(
            conn_id=conn_id, catalogs=test_catalogs, select_all_fields=select_all_fields
        )

        catalogs = menagerie.get_catalogs(conn_id)

        # Ensure our selection affects the catalog
        expected_selected = [tc.get('stream_name') for tc in test_catalogs]
        for cat in catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat['stream_id'])

            # Verify all testable streams are selected
            selected = catalog_entry.get('annotated-schema').get('selected')
            print("Validating selection on {}: {}".format(cat['stream_name'], selected))
            if cat['stream_name'] not in expected_selected:
                self.assertFalse(selected, msg="Stream selected, but not testable.")
                continue # Skip remaining assertions if we aren't selecting this stream
            self.assertTrue(selected, msg="Stream not selected.")

            if select_all_fields:
                # Verify all fields within each selected stream are selected
                for field, field_props in catalog_entry.get('annotated-schema').get('properties').items():
                    field_selected = field_props.get('selected')
                    print("\tValidating selection on {}.{}: {}".format(
                        cat['stream_name'], field, field_selected))
                    self.assertTrue(field_selected, msg="Field not selected.")
            else:
                # Verify only automatic fields are selected
                expected_automatic_fields = self.expected_automatic_fields().get(cat['stream_name'])
                selected_fields = self.get_selected_fields_from_metadata(catalog_entry['metadata'])
                self.assertEqual(expected_automatic_fields, selected_fields)
    
    @staticmethod
    def get_selected_fields_from_metadata(metadata):
        selected_fields = set()
        for field in metadata:
            is_field_metadata = len(field['breadcrumb']) > 1
            inclusion_automatic_or_selected = (
                field['metadata']['selected'] is True or \
                field['metadata']['inclusion'] == 'automatic'
            )
            if is_field_metadata and inclusion_automatic_or_selected:
                selected_fields.add(field['breadcrumb'][1])
        return selected_fields

    def timedelta_formatted(self, dtime, days=0):
        try:
            date_stripped = dt.strptime(dtime, self.START_DATE_FORMAT)
            return_date = date_stripped + timedelta(days=days)

            return dt.strftime(return_date, self.START_DATE_FORMAT)

        except ValueError:
                return Exception("Datetime object is not of the format: {}".format(self.START_DATE_FORMAT))