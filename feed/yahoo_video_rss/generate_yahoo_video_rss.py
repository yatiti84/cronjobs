from datetime import datetime
from google.cloud import storage
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from lxml.etree import CDATA, tostring
import __main__
import argparse
import gzip
import logging
import lxml.etree as ET
import yaml
import hashlib
import re

CONFIG_KEY = 'config'
GRAPHQL_CMS_CONFIG_KEY = 'graphqlCMS'
NUMBER_KEY = 'number'

yaml_parser = argparse.ArgumentParser(
    description='Process configuration of generate_yahoo_video_rss')
yaml_parser.add_argument('-c', '--config', dest=CONFIG_KEY,
                         help='config file for generate_yahoo_video_rss', metavar='FILE', type=str)
yaml_parser.add_argument('-g', '--config-graphql', dest=GRAPHQL_CMS_CONFIG_KEY,
                         help='graphql config file for generate_yahoo_video_rss', metavar='FILE', type=str, required=True)
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

# To retrieve the latest 100 published posts
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

__gql_query__ = gql(__gql_post_template__ %(config['postWhereFilter'], number))
__result__ = __gql_client__.execute(__gql_query__)
print(__result__)

def stringWrapper(name, s):
    if name in ['title', 'content', 'author']:
        return CDATA(s)
    else:
        return s
def sub(parentItem, tag, content=None):
    element = ET.SubElement(parentItem, tag)
    if content:
        element.text = stringWrapper(tag, content)
    return element


def upload_data(bucket_name: str, data: bytes, content_type: str, destination_blob_name: str):
    '''Uploads a file to the bucket.'''


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
    media = config['media']
    dcterms = config['dcterms']
    feed = config['feed']
    root = ET.Element('rss', nsmap={'media':media, 'dcterms':dcterms}, version='2.0')
    channel = sub(root, 'channel')
    sub(channel, 'title', feed['title'])
    sub(channel, 'link', feed['link'])
    sub(channel, 'description', feed['description'])
    sub(channel, 'language', feed['language'])
    sub(channel, 'copyright', feed['copyright'])
    image = sub(channel, 'image')
    sub(image, 'title', feed['image']['title'])
    sub(image, 'url', feed['image']['url'])
    sub(image, 'link', feed['image']['link'])
    articles = __result__['allVideos']
    for article in articles:
        item = ET.SubElement(channel, 'item')
        sub(item, 'title', article['name'])
        sub(item, 'link', article['url'])
        if article['description']:
            sub(item, 'description', article['description'])
        if article['categories']:
            sub(item, 'category', article['categories'][0]['name'])
        if re.search(config['baseURL'], article['url']):
            ET.SubElement(_tag='{%s}content' % media,_parent=item, nsmap={'media':media}, type="video/mp4", medium="video", url=article['url'], isDefault="true")
        media_credit = ET.SubElement(_tag='{%s}credit' % media,_parent=item, nsmap={'media':media}, role="author")
        media_credit.text = CDATA(feed['title'])
        media_credit = ET.SubElement(_tag='{%s}keywords' % media,_parent=item, nsmap={'media':media})
        guid = sub(item, 'guid', hashlib.sha224((article['url']).encode()).hexdigest())
        guid.set('isPermaLink','false')
        sub(item, 'pubDate', datetime.strptime(article['createdAt'], '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%A, %d %B %Y %H:%M:%S +0800'))

    data = ET.tostring(root, encoding="unicode")
    file_config = config['file']
    bucket_name = file_config['gcsBucket']
    rss_base = file_config['filePathBase']

    upload_data(
        bucket_name=bucket_name,
        data=data.encode('utf-8'),
        content_type='application/xml; charset=utf-8',
        destination_blob_name=rss_base +
        f'/{file_config["filenamePrefix"]}.{file_config["extension"]}'
    )
   
print(f'[{__main__.__file__}] exiting... goodbye...')
