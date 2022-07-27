from datetime import datetime, timedelta
from dateutil import parser
from google.cloud import storage
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from lxml.etree import CDATA, tostring
import __main__
import argparse
import gzip
import logging
import lxml.etree as ET
import pytz
import time
import uuid
import yaml
import re

print(f'[{__main__.__file__}] executing...')

CONFIG_KEY = 'config'
GRAPHQL_CMS_CONFIG_KEY = 'graphqlCMS'
NUMBER_KEY = 'number'

yaml_parser = argparse.ArgumentParser(
    description='Process configuration of generate_line_today_video_xml')
yaml_parser.add_argument('-c', '--config', dest=CONFIG_KEY,
                         help='config file for generate_line_today_video_xml', metavar='FILE', type=str)
yaml_parser.add_argument('-g', '--config-graphql', dest=GRAPHQL_CMS_CONFIG_KEY,
                         help='graphql config file for generate_line_today_video_xml', metavar='FILE', type=str, required=True)
yaml_parser.add_argument('-m', '--max-number', dest=NUMBER_KEY,
                         help='number of feed items', metavar='25', type=int, required=True)
args = yaml_parser.parse_args()

with open(getattr(args, CONFIG_KEY), 'r') as stream:
    config = yaml.safe_load(stream)
with open(getattr(args, GRAPHQL_CMS_CONFIG_KEY), 'r') as stream:
    config_graphql = yaml.safe_load(stream)
number = getattr(args, NUMBER_KEY)


def create_authenticated_k5_client(config_graphql: dict) -> Client:
    logger = logging.getLogger(__main__.__file__)
    logger.setLevel('INFO')
    # Authenticate through GraphQL

    gql_endpoint = config_graphql['apiEndpoint']
    gql_transport = AIOHTTPTransport(url=gql_endpoint)
    gql_client = Client(transport=gql_transport, fetch_schema_from_transport=False)
    qgl_mutation_authenticate_get_token = '''
    mutation {
        authenticate: authenticateUserWithPassword(email: "%s", password: "%s") {
            token
        }
    }
    '''
    mutation = qgl_mutation_authenticate_get_token % (
        config_graphql['username'], config_graphql['password'])

    token = gql_client.execute(gql(mutation))['authenticate']['token']

    gql_transport_with_token = AIOHTTPTransport(
        url=gql_endpoint,
        headers={
            'Authorization': f'Bearer {token}'
        },
        timeout=60
    )

    return Client(
        transport=gql_transport_with_token,
        execute_timeout=60,
        fetch_schema_from_transport=False,
    )


__gql_client__ = create_authenticated_k5_client(config_graphql)

__gql_post_template__ = '''
query{
  allVideos(where:%s, first:%s, sortBy:createdAt_DESC){
    id
    name
    url
    description
    categories{
      name
    }
    createdAt
    updatedAt
    relatedPosts {
        name
        slug
        heroImage {
        	urlOriginal
        }
    }
  }
}'''

__gql_query__ = gql(__gql_post_template__ %
                    (config['postWhereFilter'], number))
__result__ = __gql_client__.execute(__gql_query__)

# Can not accept structure contains 'array of array'


def recparse(parentItem, obj):
    t = type(obj)
    if t is dict:
        for name, value in obj.items():
            subt = type(value)
            # print(name, value)
            if subt is dict:
                thisItem = ET.SubElement(parentItem, name)
                recparse(thisItem, value)
            elif subt is list:
                for item in value:
                    thisItem = ET.SubElement(parentItem, name)
                    recparse(thisItem, item)
            elif subt is not str:
                thisItem = ET.SubElement(parentItem, name)
                thisItem.text = str(value)
            else:
                thisItem = ET.SubElement(parentItem, name)
                thisItem.text = stringWrapper(name, value)
    elif t is list:
        raise Exception('unsupported structure')
    elif t is str:
        parentItem.text = obj
    return


def stringWrapper(name, s):
    if name in ['title', 'content', 'author']:
        return CDATA(s)
    else:
        return s


def tsConverter(s):
    timeorigin = parser.parse(s)
    timediff = timeorigin - datetime(1970, 1, 1, tzinfo=pytz.utc)
    return round(timediff.total_seconds() * 1000)


def upload_data(bucket_name: str, data: bytes, content_type: str, destination_blob_name: str):
    '''Uploads a file to the bucket.'''
    # bucket_name = 'your-bucket-name'
    # data = 'storage-object-content'

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.content_encoding = 'gzip'
    print(f'[{__main__.__file__}] uploadling data to gs://{bucket_name}{destination_blob_name}')
    blob.upload_from_string(
        data=gzip.compress(data=data, compresslevel=9), content_type=content_type, client=storage_client)
    blob.content_language = 'zh'
    blob.cache_control = 'max-age=300,public,must-revalidate'
    blob.patch()

    print(
        f'[{__main__.__file__}] finished uploading gs://{bucket_name}{destination_blob_name}')


if __name__ == '__main__':
    mainXML = {
        'UUID': str(uuid.uuid4()),
        'time': int(round(time.time() * 1000)),
        'article': []
    }
    articles = __result__['allVideos']
    news_available_days = 365
    base_url = config['baseURL']
    for article in articles:
        availableDate = max(tsConverter(
            article['createdAt']), tsConverter(article['updatedAt']))
        title = re.sub(
            u'[^\u0020-\uD7FF\u0009\u000A\u000D\uE000-\uFFFD\U00010000-\U0010FFFF]+', '', article['name'])
        item = {
            'ID': article['id'],
            'nativeCountry': 'TW',
            'language': 'zh',
            'startYmdtUnix': availableDate,
            'endYmdtUnix': tsConverter(article['createdAt']) + (round(timedelta(news_available_days, 0).total_seconds()) * 1000),
            'title': title,
        }
        if article['categories']:
            item['category'] = article['categories'][0]['name']
        item['publishTimeUnix'] = availableDate
        item['contentType'] = 5
        if article['description'] is not None:
            content = re.sub(
                u'[^\u0020-\uD7FF\u0009\u000A\u000D\uE000-\uFFFD\U00010000-\U0010FFFF]+', '', article['description'])
        else:
            content = ''
        item['contents'] = {'text': {'content': content}}
        item['video'] = {'url': article['url']}
        if article['relatedPosts']:
            recommendArticles = []
            for relatedPost in article['relatedPosts'][:6]:
                if relatedPost:
                    recommendArticle = {
                        'title': relatedPost['name'], 'url': base_url + relatedPost['slug'] + config['feed']['item']['utmSource']}
                    if relatedPost['heroImage'] is not None:
                        recommendArticle['thumbnail'] = relatedPost['heroImage']['urlOriginal']
                    recommendArticles.append(recommendArticle)
            item['recommendArticles'] = {'article': recommendArticles}
        item['author'] = config['feed']['item']['author']
        mainXML['article'].append(item)

    root = ET.Element('articles')
    recparse(root, mainXML)

    data = '''<?xml version="1.0" encoding="UTF-8" ?>
    %s
    ''' % ET.tostring(root, encoding="unicode")

    file_config = config['file']
    bucket_name = file_config['gcsBucket']
    rss_base = file_config['filePathBase']

    print(f'[{__main__.__file__}] generated xml: {data}')

    upload_data(
        bucket_name=bucket_name,
        data=data.encode('utf-8'),
        content_type='application/xml; charset=utf-8',
        destination_blob_name=rss_base +
        f'/{file_config["filenamePrefix"]}.{file_config["extension"]}'
    )
print(f'[{__main__.__file__}] exiting... goodbye...')
