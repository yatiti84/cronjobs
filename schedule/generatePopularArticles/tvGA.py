from apiclient.discovery import build
import json

from google.cloud import storage
from datetime import date, timedelta, datetime

import gql


def initialize_analyticsreporting():
    """Initializes an analyticsreporting service object.

    Returns:
        analytics an authorized analyticsreporting service object.
    """

    # Build the service object.
    analytics = build('analyticsreporting', 'v4')

    return analytics


def get_report(analytics):
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
                    'dimensions': [{'name': "ga:pagePathLevel1"}, {'name': 'ga:pagePathLevel2'}],
                    'dimensionFilterClauses': [
                        {
                            'filters': [
                                {
                                    'dimensionName': 'ga:pagePath',
                                    'operator': "REGEXP",
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


def path_join(lst):
    return '/'.join([path.replace('/', '') for path in lst])


def upload_blob(bucket_name="static-mnews-tw-dev", source_file_name=FILE_NAME, destination_blob_name=FILE_NAME):
    """Uploads a file to the bucket."""
    # source_file_name = "local/path/to/file"
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f"json/{destination_blob_name}")
    # blob.upload_from_file()

    blob.upload_from_filename(source_file_name)

    print(f"File {source_file_name} uploaded to json/{destination_blob_name}.")


def jsonify_response(response):
    """Parse the response and generate the json format file for it"""
    result = {}
    with open('popularlist.json', 'w') as f:
        data = response['reports'][0]['data']['rows']
        data = sorted(
            data, key=lambda x: x['metrics'][0]['values'][0], reverse=True)
        slugs = [item['dimensions'][1].replace('/', '') for item in data]

        result['report'] = gql.gql_query_from_slugs(slugs)
        result['start_date'] = str(START_DATE)
        result['end_date'] = str(END_DATE)
        result['generate_time'] = str(datetime.now())

        f.write(json.dumps(result))


def main():
    analytics = initialize_analyticsreporting()
    response = get_report(analytics)
    jsonify_response(response)
    upload_blob()


if __name__ == '__main__':
    main()
