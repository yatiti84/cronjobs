import __main__
import argparse
import yaml
import logging
import datetime
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from google.cloud import storage

CONFIG_KEY = 'config'
GRAPHQL_CMS_CONFIG_KEY = 'graphqlCMS'
NUMBER_KEY = 'number'

yaml_parser = argparse.ArgumentParser(
description='Process configuration of mnews_sitemap_xml')
yaml_parser.add_argument('-c', '--config', dest=CONFIG_KEY,
                        help='config file for mnews_sitemap_xml', metavar='FILE', type=str)
yaml_parser.add_argument('-g', '--config-graphql', dest=GRAPHQL_CMS_CONFIG_KEY,
                        help='graphql config file for mnews_sitemap_xml', metavar='FILE', type=str, required=True)
yaml_parser.add_argument('-m', '--max-number', dest=NUMBER_KEY,
                        help='number of feed items', metavar='75', type=int, required=True)
args = yaml_parser.parse_args()

with open(getattr(args, CONFIG_KEY), 'r') as stream:
    config = yaml.safe_load(stream)
with open(getattr(args, GRAPHQL_CMS_CONFIG_KEY), 'r') as stream:
    config_graphql = yaml.safe_load(stream)
number = getattr(args, NUMBER_KEY)

print(f'[{__main__.__file__}] executing...')

__base_url__ = config['base_url']

def create_authenticated_k5_client(config_graphql: dict) -> Client:
    # logger = logging.getLogger(__main__.__file__)
    # logger.setLevel('INFO')
    # Authenticate through GraphQL

    gql_endpoint = config_graphql['apiEndpoint']
    gql_transport = AIOHTTPTransport(url=gql_endpoint)
    gql_client = Client(transport=gql_transport,fetch_schema_from_transport=False)
    qgl_mutation_authenticate_get_token = '''
    mutation{
      authenticateUserWithPassword(email:"%s", password: "%s"){
      token 
    }
  }
  '''
    mutation = qgl_mutation_authenticate_get_token % (config_graphql['username'], config_graphql['password'])

    token = gql_client.execute(gql(mutation))['authenticateUserWithPassword']['token']
    print(token)
    
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

def query_show_slug(endpoints, gql_client):
    query_show = '''
    query{
	allShows(sortBy:sortOrder_ASC){
    slug
  }
}
    '''
    query = gql(query_show)
    allShows = gql_client.execute(query)
    for item in allShows['allShows']:
        slug = item['slug']
        show = '/show/' + slug
        endpoints.append(show)


def query_cate_slug(endpoints, gql_client):
    categories = []
    query_cate = '''
    query{
	allCategories(sortBy:sortOrder_ASC){
    slug
  }
}
'''
    query = gql(query_cate)
    allCategories = gql_client.execute(query)
    for item in allCategories['allCategories']:
        slug = item['slug']
        categories.append(slug)
        if slug == 'ombuds':
            endpoints.append('/ombuds')
            continue
        if slug == 'stream':
            slug = 'video'
        cate = '/category/' + slug
        endpoints.append(cate)
    print(endpoints)
    return categories

def querty_leatest_slug(endpoints, gql_client):
 
    query_leatest = '''
    query{
    allPosts(where:{
      state:published}, sortBy:publishTime_DESC, first:120){
        slug

    }
  }'''
    query = gql(query_leatest)
    allPosts = gql_client.execute(query)
    for item in allPosts['allPosts']:
        slug = item['slug']
        post = '/story/' + slug
        endpoints.append(post)

def query_post_slug(cate, gql_client):
    print(cate)
    post_endpoint = []
    query_post = '''query{
    allPosts(where:{state:published, categories_some:{slug:"%s"}
    }, sortBy:publishTime_DESC, first:75){
    slug
    }
}''' % cate
    query = gql(query_post)
    print(query_post)
    allPosts = gql_client.execute(query)
    for item in allPosts['allPosts']:
        slug = item['slug']
        post = '/story/' + slug
        post_endpoint.append(post)
    return post_endpoint

__template__ = config['template']
def generate_sitemap_xml(endpoint_slug):
    sitemap_template = __template__['sitmap']
    sitemap = ''
    lastmod = datetime.datetime.now().strftime('%Y-%m-%d')
    sitemap += sitemap_template['header']
    for slug in endpoint_slug:
        loc = __base_url__ + slug
        #print(__base_url__ + slug)
        if slug == '/':
            priority = 1.0
        else:
            priority = 0.5
        url_tag = sitemap_template['urltag'].format(loc, lastmod, priority)
        sitemap += url_tag
    sitemap += sitemap_template['endtag']
    print(sitemap)
    return sitemap
__file_config__ = config['file']
__bucket_name__ = __file_config__['bucket_name']
__destination_prefix__ = __file_config__['destination_prefix']
def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client().from_service_account_json('./gcskeyfile.json')
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    blob.content_language = 'zh'
    blob.cache_control = 'max-age=300,public'
    blob.content_type = 'application/xml; charset=utf-8'
    blob.patch()

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )
def sitemap():
    sitemap_index_url = []
    gql_client = create_authenticated_k5_client(config_graphql)

    # made homepage sitemap
    hp_endpoint_slug = config['configs_endpoint']
    categories = query_cate_slug(hp_endpoint_slug, gql_client)
    query_show_slug(hp_endpoint_slug, gql_client)
    querty_leatest_slug(hp_endpoint_slug, gql_client)
    homepage_sitemap = generate_sitemap_xml(hp_endpoint_slug)
    print(homepage_sitemap)
    # store and upload homepage sitemap
    source_file_name = 'sitemap_homepage.xml'
    destination_blob_name = __destination_prefix__ + source_file_name
    with open(source_file_name, 'w', encoding='utf8') as f:
        f.write(homepage_sitemap)
    upload_blob(__bucket_name__, source_file_name, destination_blob_name)
    #store_sitemap_index url
    sitemap_index_url.append('/' + destination_blob_name)

    #made category sitemap
    for cate in categories:
        if cate != 'stream':
            post_endpoint_slug = query_post_slug(cate, gql_client)
            #print(post_endpoint)
            post_sitemap = generate_sitemap_xml(post_endpoint_slug)
            source_file_name = 'sitemap_{}.xml'.format(cate)
            destination_blob_name = __destination_prefix__ + source_file_name
            with open(source_file_name, 'w', encoding='utf8') as f:
                f.write(post_sitemap)
            upload_blob(__bucket_name__, source_file_name, destination_blob_name)
            sitemap_index_url.append('/' + destination_blob_name)
    return sitemap_index_url

def generate_sitemap_index_xml(sitemap_index_url):
    sitemap_index_template = __template__['sitemap_index']
    index_sitemap = ''
    lastmod = datetime.datetime.now().strftime('%Y-%m-%d')
    index_sitemap += sitemap_index_template['header']
    for slug in sitemap_index_url:
        loc = __base_url__ + slug
        #print(__base_url__ + slug)
        sitemap_tag = sitemap_index_template['urltag'].format(loc, lastmod)
        
        index_sitemap += sitemap_tag

    index_sitemap += sitemap_index_template['endtag']
    print(index_sitemap)
    return index_sitemap



sitemap_index_url = sitemap()
print(sitemap_index_url)
index_sitemap =  generate_sitemap_index_xml(sitemap_index_url)
source_file_name = 'sitemap_index.xml'
destination_blob_name = __destination_prefix__ + source_file_name
with open(source_file_name, 'w', encoding='utf8') as f:
    f.write(index_sitemap)
upload_blob(__bucket_name__, source_file_name, destination_blob_name)





