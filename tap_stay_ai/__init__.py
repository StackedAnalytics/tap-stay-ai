#!/usr/bin/env python3
import logging
import os
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, List

import requests
import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema


REQUIRED_CONFIG_KEYS = ["start_date", "api_key"]
STAY_API_URL = 'https://api.retextion.com/api/v2/subscriptions'
LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        stream_metadata = []
        key_properties = []
        replication_key = None
        if stream_id == 'subscriptions':
            key_properties = ['id']
            stream_metadata = [
                {
                    "metadata": {
                        "selected": True
                    },
                    "breadcrumb": []
                }
            ]
            replication_key = 'updatedAt'
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata,
                replication_key=replication_key,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=None,
            )
        )
    return Catalog(streams)


def sync(config, state, catalog):
    """ Sync data from tap source """

    def get_subscriptions(minimum_updated_at_datetime: datetime = None,
                          maximum_updated_at_datetime: datetime = None) -> List[Dict]:
        # TODO: Handle pagination
        subscriptions = []
        params = {}
        if minimum_updated_at_datetime:
            params['updatedAtMin'] = int(minimum_updated_at_datetime.timestamp() * 1000)
        if maximum_updated_at_datetime:
            params['updatedAtMax'] = int(maximum_updated_at_datetime.timestamp() * 1000)

        headers = {
            'X-RETEXTION-ACCESS-TOKEN': config['api_key']
        }

        response = requests.get(STAY_API_URL, headers=headers, params=params)
        response_json = response.json()
        subscriptions.extend(response_json['data'])
        return subscriptions

    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)
        bookmark_column = stream.replication_key
        is_sorted = False

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema.to_dict(),
            key_properties=stream.key_properties,
        )

        if config['start_date']:
            start_date = datetime.strptime(config['start_date'], '%Y-%m-%dT%H:%M:%SZ')
        else:
            start_date = datetime.now(tz=timezone.utc) - timedelta(hours=1)

        data = []
        if stream.tap_stream_id == 'subscriptions':
            data = get_subscriptions(start_date)

        max_bookmark = None
        for row in data:
            # write one or more rows to the stream:
            singer.write_records(stream.tap_stream_id, [row])
            if bookmark_column:
                if is_sorted:
                    # update bookmark to latest value
                    singer.write_state({stream.tap_stream_id: row[bookmark_column]})
                else:
                    # if data unsorted, save max value until end of writes
                    if max_bookmark is None:
                        max_bookmark = row[bookmark_column]
                    else:
                        max_bookmark = max(max_bookmark, row[bookmark_column])
        if bookmark_column and not is_sorted:
            singer.write_state({stream.tap_stream_id: max_bookmark})
    return


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
