#!/usr/bin/env python3
import logging
import os
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import requests
import singer
from singer import utils
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

MINIMUM_UPDATED_AT_DATETIME_CONFIG_KEY = 'minimum_updated_at_datetime'
MAXIMUM_UPDATED_AT_DATETIME_CONFIG_KEY = 'maximum_updated_at_datetime'
DATETIME_PARSE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
REQUIRED_CONFIG_KEYS = ["start_date", "access_token"]
STAY_API_URL = 'https://api.retextion.com/api/v2/'
LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + str(filename)
        file_raw = str(filename).replace('.json', '')
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


def query_stay_ai_subscriptions(access_token: str, page: int = 1, page_size: int = 50, updated_at_min: Optional[int] = None, updated_at_max: Optional[int] = None):
    logging.info(f'Fetching subscriptions: page_size={page_size}, page={page}')
    params = {
        'pageSize': page_size,
        'page': page
    }
    if updated_at_min:
        params['updatedAtMin'] = updated_at_min
    if updated_at_max:
        params['updatedAtMax'] = updated_at_max

    headers = {
        'X-RETEXTION-ACCESS-TOKEN': access_token
    }

    response = requests.get(STAY_API_URL+'/subscriptions', headers=headers, params=params)
    response_json = response.json()
    logging.info(f'Fetched subscriptions: total_in_page={len(response_json["data"])}, total={response_json["total"]}')
    return response_json['data']


def subscriptions_generator(access_token: str,
                            minimum_updated_at_datetime: datetime = None,
                            maximum_updated_at_datetime: datetime = None):
    page = 1
    page_size = 50
    while True:
        subscriptions = query_stay_ai_subscriptions(
            access_token=access_token,
            page=page,
            updated_at_min=int(minimum_updated_at_datetime.timestamp() * 1000) if minimum_updated_at_datetime else None,
            updated_at_max=int(maximum_updated_at_datetime.timestamp() * 1000) if maximum_updated_at_datetime else None
        )
        yielded_subscriptions = 0
        for subscription in subscriptions:
            yield subscription
            yielded_subscriptions += 1
        if yielded_subscriptions < page_size:
            return
        page += 1


def sync(config, state, catalog):
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)
        bookmark_column = stream.replication_key

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema.to_dict(),
            key_properties=stream.key_properties,
        )

        min_updated_at_datetime = datetime.now(tz=timezone.utc) - timedelta(days=1)
        max_updated_at_datetime = None
        if state.get('bookmarks', {}).get('subscriptions', {}).get('last_record'):
            min_updated_at_datetime = state['bookmarks']['subscriptions']['last_record']
        elif config['start_date']:
            min_updated_at_datetime = datetime.strptime(config['start_date'], DATETIME_PARSE_FORMAT)

        if config.get(MINIMUM_UPDATED_AT_DATETIME_CONFIG_KEY):
            min_updated_at_datetime = datetime.strptime(config[MINIMUM_UPDATED_AT_DATETIME_CONFIG_KEY], DATETIME_PARSE_FORMAT)

        if config.get(MAXIMUM_UPDATED_AT_DATETIME_CONFIG_KEY):
            max_updated_at_datetime = datetime.strptime(config[MAXIMUM_UPDATED_AT_DATETIME_CONFIG_KEY], DATETIME_PARSE_FORMAT)

        generator = []
        if stream.tap_stream_id == 'subscriptions':
            generator = subscriptions_generator(
                access_token=config['access_token'],
                minimum_updated_at_datetime=min_updated_at_datetime,
                maximum_updated_at_datetime=max_updated_at_datetime
            )

        max_bookmark = None
        for row in generator:
            singer.write_records(stream.tap_stream_id, [row])
            if bookmark_column:
                if max_bookmark is None:
                    max_bookmark = row[bookmark_column]
                else:
                    max_bookmark = max(max_bookmark, row[bookmark_column])
                    singer.write_state({stream.tap_stream_id: row[bookmark_column]})
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
