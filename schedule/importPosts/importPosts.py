from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from mergedeep import merge, Strategy
import argparse
import json
import logging
import sys
import urllib
import yaml

CONFIG_KEY = 'config'
GRAPHQL_CMS_CONFIG_KEY = 'graphqlCMS'
MAX_NUMBER_KEY = 'maxNumber'

__defaultConfig = {
    "sourceK3Endpoints":
    {
        "list": "https://api.mirrormedia.mg/getlist",
        "posts": "https://api.mirrormedia.mg/getposts",
    },
    "author": "鏡週刊",
    "destSlugPrefix": "mm-",
}

__defaultgraphqlCmsConfig = {
    'username': '',
    'password': '',
    'apiEndpoint': '',
}

__query_existing_posts_template = '''query {
  allPosts(where: {AND: [{OR: [%s]}]}) {
    slug
  }
}
'''


def get_k3_posts(k3_endpoint: str, max_results: int = 20, sort: str = '-publishedDate', populate: str = 'categories,heroImage') -> dict:
    '''getK3Posts get posts from k3'''
    logger = logging.getLogger(__name__)
    url = f'{k3_endpoint}?where={{"state":"published"}}&max_results={max_results}&sort={sort}&populate={populate}'
    logger.info(f'sending request:{url}')
    print
    req = urllib.request.Request(
        url=url, headers={'Accept': 'application/json;charset=utf-8'})
    resp = urllib.request.urlopen(req)
    if resp.status < 200 and resp.status >= 300:
        with resp as f:
            raise f'[{__file__}] response from {req.get_full_url()} has status({resp.status}), resp body:{f.read().decode("utf-8")}'

    with resp as f:
        return json.loads(f.read().decode('utf-8'))['_items']


def find_existing_slugs_set(config_graphql: dict = None, slugs: list = []) -> set:

    gql_endpoint = config_graphql['apiEndpoint']
    gql_transport = RequestsHTTPTransport(
        url=gql_endpoint,
        use_json=True,
        headers={
            "Content-Type": "application/json",
        },
        verify=True,
        retries=3,
    )
    gql_client = Client(
        transport=gql_transport,
        fetch_schema_from_transport=False,
    )

    # format query array
    query_conditions = ','.join(
        ['{slug: "%s"}' % slug for slug in slugs])
    query = __query_existing_posts_template % query_conditions
    logger.info(f'query slugs for existence:{query}')
    # extract slugs
    existing_slugs = [post['slug'] for post in gql_client.execute(gql(query))[
        'allPosts']]
    return set(existing_slugs)


def is_category_not_member_only(category: dict) -> bool:
    # is_member_only mey not be presented. In such case, we treat it as False.
    return category.get('is_member_only') in (None, False)


def main(config: dict = None, config_graphql: dict = None, playlist_ids: list = None, max_number: int = 3):
    ''' Import YouTube Channel program starts here '''
    logger = logging.getLogger(__name__)

    # 1. request https://api.mirrormedia.mg/getposts?where={"state": "published"}&max_results=100&sort=-publishedDate&populate=categories,heroImage
    posts = get_k3_posts(
        k3_endpoint=config['sourceK3Endpoints']['posts'], max_results=max_number)
    # 2. Check post existence
    posts_with_new_slug = posts
    for i in range(len(posts_with_new_slug)):
        posts_with_new_slug[i]['slug'] = f'{config["destSlugPrefix"]}{posts_with_new_slug[i]["slug"]}'

    slugs = [post['slug'] for post in posts_with_new_slug]
    existing_slugs_set = find_existing_slugs_set(
        config_graphql=config_graphql, slugs=slugs)

    new_posts = [
        post for post in posts_with_new_slug if f'{posts["slug"]}' not in existing_slugs_set and (all([is_category_not_member_only(c) for c in post.get('categories', [])]))]

    logger.info(f'news posts:{new_posts}')
    # 3. Clean Post
    # 4. Check hero image existence
    # 5. Insert post only or insert post and image together

    # merge option to the default configs
    config = merge({}, __defaultConfig, config,
                   strategy=Strategy.TYPESAFE_REPLACE)
    config_graphql = merge({}, __defaultgraphqlCmsConfig, config_graphql,
                           strategy=Strategy.TYPESAFE_REPLACE)


logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logger.info(f'{__file__} is executing...')
    parser = argparse.ArgumentParser(
        description='Process configuration of importPosts')
    parser.add_argument('-c', '--config', dest=CONFIG_KEY,
                        help='config file for importPosts', metavar='FILE', type=str)
    parser.add_argument('-g', '--config-graphql', dest=GRAPHQL_CMS_CONFIG_KEY,
                        help='graphql config file for importPosts', metavar='FILE', type=str, required=True)
    parser.add_argument('-m', '--max-number', dest=MAX_NUMBER_KEY,
                        help='max number of posts', metavar='10', type=int, required=True)

    args = parser.parse_args()

    with open(getattr(args, CONFIG_KEY), 'r') as stream:
        config = yaml.safe_load(stream)
    with open(getattr(args, GRAPHQL_CMS_CONFIG_KEY), 'r') as stream:
        config_graphql = yaml.safe_load(stream)
    max_number = getattr(args, MAX_NUMBER_KEY)

    main(config, config_graphql, max_number)
