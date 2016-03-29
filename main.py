import os
import boto3
import click
import logging
from copy import copy
from collections import OrderedDict
from datetime import date, timedelta
from sentinel_s3 import range_metadata, s3_writer

from elasticsearch import Elasticsearch, RequestError


def create_index(index_name, doc_type):

    body = {
        doc_type: {
            'properties': {
                'scene_id': {'type': 'string', 'index': 'not_analyzed'},
                'satellite_name': {'type': 'string'},
                'cloud_coverage': {'type': 'float'},
                'date': {'type': 'date'},
                'data_geometry': {
                    'type': 'geo_shape',
                    'tree': 'quadtree',
                    'precision': '5mi'}
            }
        }
    }

    es.indices.create(index=index_name, ignore=400)

    es.indices.put_mapping(
        doc_type=doc_type,
        body=body,
        index=index_name
    )


def elasticsearch_updater(product_dir, metadata):

    # es.indices.create(index='satellites', ignore=400)

    internal_meta = copy(metadata)

    body = OrderedDict({
        'scene_id': internal_meta.pop('tile_name'),
        'satellite_name': metadata.get('spacecraft_name', 'Sentintel-2A'),
        'cloud_coverage': metadata.get('cloudy_pixel_percentage', 100),
    })

    body.update(internal_meta)

    body['data_geometry'] = body.pop('tile_data_geometry')

    try:
        es.index(index="satellites", doc_type="sentinel2", id=body['scene_id'],
                 body=body)
    except RequestError:
        body['data_geometry'] = None
        es.index(index="satellites", doc_type="sentinel2", id=body['scene_id'],
                 body=body)


def last_updated(today):
    """ Gets the latest time a product added to S3 bucket """

    bucket_name = os.getenv('BUCKETNAME', 'sentinel-metadata')
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    start_day = today.day
    start_month = today.month

    yr_counter = 0
    while True:
        m_counter = 0
        year = today.year - yr_counter
        if year < 2015:
            break
        while True:
            month = start_month - m_counter
            if month == 0:
                start_month = 12
                break
            d_counter = 0
            while True:
                day = start_day - d_counter
                if day == 0:
                    start_day = 31
                    break
                path = os.path.join(str(year), str(month), str(day))
                print('checking %s' % path)
                objs = bucket.objects.filter(Prefix=path).limit(1)
                if list(objs):
                    return date(year, month, day)
                d_counter += 1
            m_counter += 1
        yr_counter += 1

    return None


@click.command()
@click.argument('ops', metavar='<operations: choices: s3 | es>', nargs=-1)
@click.option('--start', default=None, help='Start Date. Format: YYYY-MM-DD')
@click.option('--end', default=None, help='End Date. Format: YYYY-MM-DD')
@click.option('--concurrency', default=20, type=int, help='End Date. Format: YYYY-MM-DD')
@click.option('--es-host', default='localhost', help='Elasticsearch host address')
@click.option('--es-port', default=9200, type=int, help='Elasticsearch port number')
@click.option('-v', '--verbose', default=False, type=bool)
def main(ops, start, end, concurrency, es_host, es_port, verbose):

    accepted_args = {
        'es': elasticsearch_updater,
        's3': s3_writer
    }

    writers = []
    for op in ops:
        if op in accepted_args.keys():
            writers.append(accepted_args[op])
        else:
            raise click.UsageError('Operation (%s) is not supported' % op)

    logger = logging.getLogger('sentinel.meta.s3')
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()

    if verbose:
        ch.setLevel(logging.INFO)
    else:
        ch.setLevel(logging.ERROR)

    formatter = logging.Formatter('%(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    if not end:
        end = date.today()

    if not start:
        delta = timedelta(days=3)
        start = end - delta

    if 'es' in ops:
        global es
        es = Elasticsearch([{
            'host': es_host,
            'port': es_port
        }])

        create_index('satellites', 'sentinel2')

    range_metadata(start, end, '.', concurrency, writers)


if __name__ == '__main__':
    main()
