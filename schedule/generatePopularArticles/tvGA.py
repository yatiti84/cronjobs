from apiclient.discovery import build
from datetime import date, timedelta, datetime
from google.cloud import storage
from mergedeep import merge, Strategy
import argparse
import gql
import gzip
import json
import yaml


def initialize_analyticsreporting() -> googleapiclient.discovery.Resource:
    '''Initializes an analyticsreporting service object.

    Returns:
        analytics an authorized analyticsreporting service object.
    '''

    # Build the service object.
    analytics = build('analyticsreporting', 'v4')

    return analytics


def get_report(analytics: googleapiclient.discovery.Resource, analyticsID: str, daydelta: int) -> dict:
    # Use the Analytics Service Object to query the Analytics Reporting API V4.
    return analytics.reports().batchGet(
        body={
            'reportRequests': [
                {
                    'viewId': analyticsID,
                    'dateRanges': [{'startDate': str(date.today() - timedelta(days=daydelta)), 'endDate': str(date.today())}],
                    'metrics': [
                        {'expression': 'ga:pageviews'}
                    ],
                    'orderBys': [{'fieldName': 'ga:pageviews', 'sortOrder': 'DESCENDING'}],
                    'dimensions': [{'name': 'ga:pagePathLevel1'}, {'name': 'ga:pagePathLevel2'}],
                    'dimensionFilterClauses': [
                        {
                            'filters': [
                                {
                                    'dimensionName': 'ga:pagePath',
                                    'operator': 'REGEXP',
                                    'expressions': [
                                        '^\/story\/|^\/projects\/'
                                    ]
                                }
                            ]
                        }
                    ],
                    'pageSize': 60
                }]
        }
    ).execute()


def upload_blob(bucket_name: str, destination_blob_name: str, report: str):
    '''Uploads a string to the bucket.'''
    # destination_blob_name = 'storage-object-name'

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f'json/{destination_blob_name}')

    blob.content_encoding = 'gzip'
    blob.upload_from_string(
        data=gzip.compress(data=report, compresslevel=9), content_type='application/json', client=storage_client)
    blob.content_language = 'zh'
    blob.cache_control = 'max-age=300,public'
    blob.patch()

    print(
        f'Report is uploaded to bucket://{bucket_name}/json/{destination_blob_name}')


def convert_response_to_report(configGraphQL: dict, response: dict) -> str:
    '''Parse the response and generate the json format file for it'''
    result = {}
    data = response['reports'][0]['data']['rows']
    data = sorted(
        data, key=lambda x: x['metrics'][0]['values'][0], reverse=True)
    slugs = [item['dimensions'][1].replace('/', '') for item in data]

    result['report'] = gql.gql_query_from_slugs(
        configGraphQL, config['report']['fileHostDomainRule'], slugs)
    result['start_date'] = str(START_DATE)
    result['end_date'] = str(END_DATE)
    result['generate_time'] = str(datetime.now())

    return json.dumps(result, ensure_ascii=False)


__defaultConfig = {
    'analyticsID': '',
    'report': {
        'bucketName': '',
        'fileName': 'popularlist.json',
        'filter': ['^\/story\/|^\/projects\/'],
    },
    'fileHostDomainRule': {
        "https://storage.googleapis.com/mirrormedia-files": "https://www.mirrormedia.mg",
        "https://storage.googleapis.com/static-mnews-tw-dev": "https://dev.mnews.tw",

        "https://storage.googleapis.com/mirror-tv-file": "https://dev.mnews.tw",
    }
}

__defaultgraphqlCmsConfig = {
    'username': '',
    'password': '',
    'apiEndpoint': '',
}


def main(config: dict, configGraphQL: dict, days: int):
    print(f'{__file__} is executing...')

    # merge option to the default configs
    config = merge({}, __defaultConfig, config,
                   strategy=Strategy.TYPESAFE_REPLACE)
    config_graphql = merge({}, __defaultgraphqlCmsConfig, configGraphQL,
                           strategy=Strategy.TYPESAFE_REPLACE)
    if days <= 0:
        days = 2

    analytics = initialize_analyticsreporting()
    response = get_report(analytics, config['analyticsID'], days)
    report = convert_response_to_report(configGraphQL, response)
    upload_blob(bucket_name=config['report']['bucketName'], report=report)


CONFIG_KEY = 'config'
GRAPHQL_CMS_CONFIG_KEY = 'graphqlCMS'
DAYS_KEY = 'days'

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Process configuration of generatePopularArticles')
    parser.add_argument('-c', '--config', dest=CONFIG_KEY,
                        help='config file for generatePopularArticles', metavar='FILE', type=str)
    parser.add_argument('-g', '--config-graphql', dest=GRAPHQL_CMS_CONFIG_KEY,
                        help='graphql config file for generatePopularArticles', metavar='FILE', type=str, required=True)
    parser.add_argument('-d', '--days', dest=DAYS_KEY,
                        help='the number of days for the report before now', metavar='2', type=int, required=True)

    args = parser.parse_args()

    with open(getattr(args, CONFIG_KEY), 'r') as stream:
        config = yaml.safe_load(stream)
    with open(getattr(args, GRAPHQL_CMS_CONFIG_KEY), 'r') as stream:
        configGraphQL = yaml.safe_load(stream)
    days = getattr(args, DAYS_KEY)

    main(config, configGraphQL, days)
