from datetime import datetime, timedelta
from google.cloud import storage
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
import __main__
import argparse
import logging
import yaml

CONFIG_KEY = 'config'
GRAPHQL_CMS_CONFIG_KEY = 'graphqlCMS'
NUMBER_KEY = 'number'


yaml_parser = argparse.ArgumentParser(
    description='Process configuration of generate_google_news_rss')
yaml_parser.add_argument('-c', '--config', dest = CONFIG_KEY,
                         help = 'config file for generate_google_news_rss', metavar = 'FILE', type=str)
yaml_parser.add_argument('-g', '--config-graphql', dest = GRAPHQL_CMS_CONFIG_KEY,
                         help = 'graphql config file for generate_google_news_rss', metavar = 'FILE', type = str, required = True)
yaml_parser.add_argument('-m', '--max-number', dest = NUMBER_KEY,
                         help = 'number of feed items', metavar = '75', type = int, required = True)
args = yaml_parser.parse_args()

with open(getattr(args, CONFIG_KEY), 'r') as stream:
    config = yaml.safe_load(stream)
with open(getattr(args, GRAPHQL_CMS_CONFIG_KEY), 'r') as stream:
    config_graphql = yaml.safe_load(stream)
number = getattr(args, NUMBER_KEY)

print(f'[{__main__.__file__}] executing...')

__base_url__ = config['base_url']
__template__ = config['template']
__file_config__ = config['file']
__bucket_name__ = __file_config__['bucket_name']
__destination_prefix__ = __file_config__['destination_prefix']
__src_file_name__ = __file_config__['src_file_name']


def create_authenticated_k5_client(config_graphql: dict) -> Client:
    logger = logging.getLogger(__main__.__file__)
    logger.setLevel('INFO')
    # Authenticate through GraphQL

    gql_endpoint = config_graphql['apiEndpoint']
    gql_transport = AIOHTTPTransport(url=gql_endpoint)
    gql_client = Client(transport=gql_transport, fetch_schema_from_transport=False)
    gql_mutation_authenticate_get_token = '''
    mutation {
        authenticate: authenticateUserWithPassword(email: "%s", password: "%s") {
            token
        }
    }
    '''
    mutation = gql_mutation_authenticate_get_token % (config_graphql['username'], config_graphql['password'])

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


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    blob.content_language = 'zh'
    blob.cache_control = 'max-age=300,public,must-revalidate'
    blob.content_type = 'application/xml; charset=utf-8'
    blob.patch()

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )


def query_cate_slug(gql_client):
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
    if isinstance(allCategories, dict) and 'allCategories' in allCategories:
        allCategories = allCategories['allCategories']
        if isinstance(allCategories, list) and allCategories:
            for item in allCategories:
                slug = item['slug']
                categories.append(slug)

        else:
            print("no cate")
    return categories


def query_post(cate, gql_client):
    posts = {}
    date_limit = (datetime.now() - timedelta(days=2)
            ).strftime('%Y-%m-%dT%H:%M:%S')
    query_post = '''query{
    allPosts(where:{state:published, categories_some:{slug:"%s"}, publishTime_gte:"%s"
    }, sortBy:publishTime_DESC, first:%s){
    slug
    name
    }
}''' % (cate, date_limit, number)
    query = gql(query_post)
    allPosts = gql_client.execute(query)
    if isinstance(allPosts, dict) and 'allPosts' in allPosts:
        allPosts = allPosts['allPosts']
        if isinstance(allPosts, list) and allPosts:
            for item in allPosts:
                slug = item['slug']
                title = item['name']
                posts[slug] = title
    return posts


def generate_sitemap_content(posts_slug_title):
    sitemap_template = __template__['sitmap']
    sitemap = sitemap_template['header']
    lastmod = datetime.datetime.now().strftime('%Y-%m-%d')
    for slug, title in posts_slug_title.items():
        loc = __base_url__ + '/story/' + slug
        url_tag = sitemap_template['urltag'].format(loc, lastmod, title)
        sitemap += url_tag
    sitemap += sitemap_template['endtag']
    return sitemap


def generate_sitemap_index_content(sitemap_index_url):
    sitemap_index_template = __template__['sitemap_index']
    index_sitemap = sitemap_index_template['header']
    lastmod = datetime.datetime.now().strftime('%Y-%m-%d')
    for slug in sitemap_index_url:
        loc = __base_url__ + slug
        sitemap_tag = sitemap_index_template['urltag'].format(loc, lastmod)

        index_sitemap += sitemap_tag

    index_sitemap += sitemap_index_template['endtag']

    return index_sitemap


def google_news():
    sitemap_cate_url = []
    gql_client = create_authenticated_k5_client(config_graphql)
    print("query post with category")
    categories = query_cate_slug(gql_client)

    if categories:
        # made category sitemap
        for cate in categories:

            posts = query_post(cate, gql_client)
            if posts:
                post_sitemap = generate_sitemap_content(posts)
            else:
                print("no post", cate)
                continue
            source_file_name = __src_file_name__['cate_post'].format(cate)
            destination_blob_name = __destination_prefix__ + source_file_name
            with open(source_file_name, 'w', encoding='utf8') as f:
                f.write(post_sitemap)
            upload_blob(__bucket_name__, source_file_name,
                        destination_blob_name)
            sitemap_cate_url.append('/' + destination_blob_name)
        sitemap_index_content = generate_sitemap_index_content(sitemap_cate_url)
        source_file_name = __src_file_name__['sitemap_index']
        destination_blob_name = __destination_prefix__ + source_file_name
        with open(source_file_name, 'w', encoding='utf8') as f:
            f.write(sitemap_index_content)
        upload_blob(__bucket_name__, source_file_name, destination_blob_name)
    else:
        print('no categories')

    return


if __name__ == '__main__':
    google_news()
    print("done")
