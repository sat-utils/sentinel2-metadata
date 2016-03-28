import os
import boto3
import logging
from copy import copy
from collections import OrderedDict
from datetime import date, timedelta
from sentinel_s3 import range_metadata, s3_writer

from elasticsearch import Elasticsearch, RequestError


es = Elasticsearch([{'host': 'xxxxxxxxx',
                     'port': 443}],
                   use_ssl=True,)


def create_index(index_name, doc_type):

    body = {
        doc_type: {
            'properties': {
                'scene_id': {'type': 'string', 'index': 'not_analyzed'},
                'satellite_name': {'type': 'string'},
                'cloud_coverage': {'type': 'float'},
                'date': {'type': 'date'},
                'data_geometry': {'type': 'geo_shape', 'tree': 'quadtree', 'precision': '5mi'}
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
        'cloud_coverage': metadata.get('cloudy_coverage_percentage', 100),
    })

    body.update(internal_meta)

    body['data_geometry'] = body.pop('tile_data_geometry')

    try:
        es.index(index="satellites", doc_type="sentinel2", id=body['scene_id'], body=body)
    except RequestError:
        body['data_geometry'] = None
        es.index(index="satellites", doc_type="sentinel2", id=body['scene_id'], body=body)


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


def main(start_date, end_date):
    return range_metadata(start_date, end_date, '.', 20, [elasticsearch_updater])


if __name__ == '__main__':
    logger = logging.getLogger('sentinel.meta.s3')
    logger.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    today = date.today()
    # update_date = last_updated(today)
    # delta = timedelta(days=1)  # reprocess data for a day before the available date

    create_index('satellites', 'sentinel2')

    # if update_date:
        # print(main(update_date - delta, today))
    print(main(date(2016, 1, 1), today))

