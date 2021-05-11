from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from mergedeep import merge, Strategy
import __main__
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
    logger = logging.getLogger(__main__.__file__)
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
    gql_transport = AIOHTTPTransport(
        url=gql_endpoint,
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


def convert_hero_image(image_src: dict, post_dest: dict):
    if image_src != None:
        post_dest['heroImage'] = {
            'name': image_src['description'],
            'filename': image_src['image']['filename'],
            'file': json.dumps(image_src['image'], ensure_ascii=False),
            'meta': image_src['image']['filetype'],
            'urlOriginal': image_src['image']['url'],
            'urlDesktopSized': image_src['image']['resizedTargets']['desktop']['url'],
            'urlMobileSized': image_src['image']['resizedTargets']['mobile']['url'],
            'urlTabletSized': image_src['image']['resizedTargets']['tablet']['url'],
            'urlTinySized': image_src['image']['resizedTargets']['tiny']['url'],
        }


def convert_and_clean_post_for_k5(posts: list, delegated_writers: list) -> list:
    new_posts = []

    for post in posts:
        new_post = {}
        convert_hero_image(post.get('heroImage', None), new_post)
        new_post['brief'] = json.dumps(
            post['brief']['draft'], ensure_ascii=False)
        new_post['briefApiData'] = post['brief']['apiData']
        new_post['briefHtml'] = post['brief']['html']
        new_post['content'] = json.dumps(
            post['content']['draft'], ensure_ascii=False)
        new_post['contentApiData'] = post['content']['apiData']
        new_post['contentHtml'] = post['content']['html']
        new_post['heroCaption'] = post.get('heroCaption', None)
        new_post['name'] = post['title']
        new_post['slug'] = post['slug']
        new_post['writers'] = delegated_writers
        new_posts.append(new_post)

    return new_posts


def is_category_not_member_only(category: dict) -> bool:
    # is_member_only mey not be presented. In such case, we treat it as False.
    return category.get('is_member_only') in (None, False)


def create_authenticated_k5_client(config_graphql: dict) -> Client:
    logger = logging.getLogger(__main__.__file__)
    # Authenticate through GraphQL

    gql_endpoint = config_graphql['apiEndpoint']
    gql_transport = AIOHTTPTransport(
        url=gql_endpoint,
    )
    gql_client = Client(
        transport=gql_transport,
        fetch_schema_from_transport=False,
    )
    qgl_mutation_authenticate_get_token = '''
    mutation {
        authenticate: authenticateUserWithPassword(email: "%s", password: "%s") {
            token
        }
    }
    '''
    mutation = gql(qgl_mutation_authenticate_get_token %
                   (config_graphql['username'], config_graphql['password']))

    token = gql_client.execute(mutation)['authenticate']['token']

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


def main(config: dict = None, config_graphql: dict = None, playlist_ids: list = None, max_number: int = 3):
    ''' Import YouTube Channel program starts here '''
    logger = logging.getLogger(__main__.__file__)

    # 1. request https://api.mirrormedia.mg/getposts?where={"state": "published"}&max_results=100&sort=-publishedDate&populate=categories,heroImage
    posts = get_k3_posts(
        k3_endpoint=config['sourceK3Endpoints']['posts'], max_results=max_number)
    # 2. Check post existence
    posts_with_new_slug = posts
    for i in range(len(posts_with_new_slug)):
        posts_with_new_slug[i]['slug'] = f'{config["destSlugPrefix"]}{posts_with_new_slug[i]["slug"]}'
        try:
            posts_with_new_slug[i]['heroImage'][
                'description'] = f'{config["destSlugPrefix"]}{posts_with_new_slug[i]["heroImage"]["description"]}'
        except:
            # in case of posts without heroimage
            pass

    slugs = [post['slug'] for post in posts_with_new_slug]
    existing_slugs_set = find_existing_slugs_set(
        config_graphql=config_graphql, slugs=slugs)

    new_posts = [
        post for post in posts_with_new_slug if f'{post["slug"]}' not in existing_slugs_set and (all([is_category_not_member_only(c) for c in post.get('categories', [])]))]

    logger.info(f'news posts:{new_posts}')
    # 3. Generate and clean up Posts for k5
    k5_posts = convert_and_clean_post_for_k5(new_posts, config['writers'])
    logger.info(f'posts generated for k5:{k5_posts}')
    # 4. Check hero image existence
    # 5. Insert post only or insert post and image together

    # merge option to the default configs
    config = merge({}, __defaultConfig, config,
                   strategy=Strategy.TYPESAFE_REPLACE)
    config_graphql = merge({}, __defaultgraphqlCmsConfig, config_graphql,
                           strategy=Strategy.TYPESAFE_REPLACE)


logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    logger = logging.getLogger(__main__.__file__)
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

    main(config=config, config_graphql=config_graphql, max_number=max_number)
