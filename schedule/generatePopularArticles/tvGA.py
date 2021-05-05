from apiclient.discovery import build
from datetime import date, timedelta, datetime
from google.cloud import storage
import gql
import gzip
import json


def initialize_analyticsreporting() -> googleapiclient.discovery.Resource:
    '''Initializes an analyticsreporting service object.

    Returns:
        analytics an authorized analyticsreporting service object.
    '''

    # Build the service object.
    analytics = build('analyticsreporting', 'v4')

    return analytics


def get_report(analytics: googleapiclient.discovery.Resource) -> dict:
    # Use the Analytics Service Object to query the Analytics Reporting API V4.
    return analytics.reports().batchGet(
        body={
            'reportRequests': [
                {
                    'viewId': GA_ID,
                    'dateRanges': [{'startDate': str(START_DATE), 'endDate': str(END_DATE)}],
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
                    'pageSize': PAGE_SIZE
                }]
        }
    ).execute()


def upload_blob(bucket_name: str = 'static-mnews-tw-dev', destination_blob_name: str = FILE_NAME, report: str = ''):
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

    print(f'Report is uploaded to json/{destination_blob_name}.')


def convert_response_to_report(response: dict) -> str:
    '''Parse the response and generate the json format file for it'''
    result = {}
    data = response['reports'][0]['data']['rows']
    data = sorted(
        data, key=lambda x: x['metrics'][0]['values'][0], reverse=True)
    slugs = [item['dimensions'][1].replace('/', '') for item in data]

    result['report'] = gql.gql_query_from_slugs(slugs)
    result['start_date'] = str(START_DATE)
    result['end_date'] = str(END_DATE)
    result['generate_time'] = str(datetime.now())

    return json.dumps(result, ensure_ascii=False)


def main():
    analytics = initialize_analyticsreporting()
    response = get_report(analytics)
    report = convert_response_to_report(response)
    upload_blob(report=report)


if __name__ == '__main__':
    main()
