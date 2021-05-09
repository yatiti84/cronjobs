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

__queryExistingVideosTemplate = '''
query {
  allVideos(where: {AND: [{OR: [%s]}, {OR: [{url_contains_i: "youtube"}, {url_contains_i: "youtu.be"}]}]}) {
    url
  }
}
'''


def get_k3_posts(k3_endpoint: str, max_results: int = 20, sort: str = '-publishedDate', populate: str = 'categories,heroImage') -> dict:
    '''getK3Posts get posts from k3'''
    logger = logging.getLogger(__name__)
    url = f'{k3_endpoint}?where={"state": "published"}&max_results={max_results}&sort={sort}&populate={populate}'
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


def main(config: dict = None, config_graphql: dict = None, playlist_ids: list = None, max_number: int = 3):
    ''' Import YouTube Channel program starts here '''

    # 1. request https://api.mirrormedia.mg/getposts?where={"state": "published"}&max_results=100&sort=-publishedDate&populate=categories,heroImage
    posts = get_k3_posts(
        k3_endpoint=config['sourceK3Endpoints']['posts'], max_results=max_number)
    # 2. Check post existence

    # 3. Clean Post
    # 4. Check hero image existence
    # 5. Insert post only or insert post and image together

    # merge option to the default configs
    config = merge({}, __defaultConfig, config,
                   strategy=Strategy.TYPESAFE_REPLACE)
    config_graphql = merge({}, __defaultgraphqlCmsConfig, config_graphql,
                           strategy=Strategy.TYPESAFE_REPLACE)


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
