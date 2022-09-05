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
import json
import sys
sys.path.append('/cronjobs')
from feed.utils import create_authenticated_k5_client, upload_data, tsConverter, recparse, replace_alt_with_descrption



print(f'[{__main__.__file__}] executing...')

CONFIG_KEY = 'config'
GRAPHQL_CMS_CONFIG_KEY = 'graphqlCMS'
NUMBER_KEY = 'number'

yaml_parser = argparse.ArgumentParser(
    description='Process configuration of generate_google_news_rss')
yaml_parser.add_argument('-c', '--config', dest=CONFIG_KEY,
                         help='config file for generate_google_news_rss', metavar='FILE', type=str)
yaml_parser.add_argument('-g', '--config-graphql', dest=GRAPHQL_CMS_CONFIG_KEY,
                         help='graphql config file for generate_google_news_rss', metavar='FILE', type=str, required=True)
yaml_parser.add_argument('-m', '--max-number', dest=NUMBER_KEY,
                         help='number of feed items', metavar='75', type=int, required=True)
args = yaml_parser.parse_args()

with open(getattr(args, CONFIG_KEY), 'r') as stream:
    config = yaml.safe_load(stream)
with open(getattr(args, GRAPHQL_CMS_CONFIG_KEY), 'r') as stream:
    config_graphql = yaml.safe_load(stream)
number = getattr(args, NUMBER_KEY)


__gql_client__ = create_authenticated_k5_client(config_graphql)

# To retrieve the latest 100 published posts
__qgl_post_template__ = '''
{
    allPosts(where: %s, sortBy: publishTime_DESC, first: %d) {
        id
        name
        slug
        briefHtml
        contentHtml
        contentApiData
        heroCaption
        heroImage {
            urlOriginal
            name
        }
        categories {
            name
            slug
        }
        relatedPosts {
            name
            slug
            heroImage {
            urlOriginal
            
            }
        }
        writers {
            name
        }
        tags{
            name
        }
        publishTime
        updatedAt
    }
}
'''

__gql_query__ = gql(__qgl_post_template__ %
                    (config['postWhereFilter'], number))
__result__ = __gql_client__.execute(__gql_query__)

def generate_heroImge_tag(article):
    if article['heroImage'] is None:
        return f"<img alt=\"logo\" src=\"{config['feed']['item']['logo_url']}\">"
    if article['heroCaption'] :
        return f"<img alt=\"{article['heroCaption']}\" src=\"{article['heroImage']['urlOriginal']}\">"
    return f"<img src=\"{article['heroImage']['urlOriginal']}\">"


if __name__ == '__main__':
    mainXML = {
        'UUID': str(uuid.uuid4()),
        'time': int(round(time.time() * 1000)),
        'article': []
    }

    articles = __result__['allPosts']

    news_available_days = 365
    base_url = config['baseURL']
    for article in articles:
        availableDate = max(tsConverter(
            article['publishTime']), tsConverter(article['updatedAt']))
        content =  generate_heroImge_tag(article)# add hero img in beginning of content
        if article['briefHtml'] is not None:
            brief = article['briefHtml']
            content += brief 
        if article['contentHtml'] is not None:
            ytb_iframe = re.search(config['feed']['item']['ytb_iframe_regex'], article['contentHtml'])
            contentHtml = re.sub(config['feed']['item']['ytb_iframe_regex'], '', article['contentHtml'])
            img_list = re.findall('<img.*?>', contentHtml)
            if img_list:
                contentHtml = replace_alt_with_descrption(contentHtml, json.loads(article['contentApiData']), img_list)
            content += contentHtml
            content = re.sub(u'[^\u0020-\uD7FF\u0009\u000A\u000D\uE000-\uFFFD\U00010000-\U0010FFFF]+', '', content)
        title = re.sub(u'[^\u0020-\uD7FF\u0009\u000A\u000D\uE000-\uFFFD\U00010000-\U0010FFFF]+', '', article['name'])
        item = {
            'ID': article['id'],
            'nativeCountry': 'TW',
            'language': 'zh',
            'startYmdtUnix': availableDate,
            'endYmdtUnix': tsConverter(article['publishTime']) + (round(timedelta(news_available_days, 0).total_seconds()) * 1000),
            'title': title,
            'category': article['categories'][0]['name'] if len(article['categories']) > 0 else [],
            'publishTimeUnix': tsConverter(article['publishTime']),
        }
        if article['updatedAt'] is not None:
            updateTimeUnix = tsConverter(article['updatedAt'])
            item['updateTimeUnix'] = updateTimeUnix
        item['contentType'] = 0
        if article['heroImage'] is not None:
            item['thumbnail'] = article['heroImage']['urlOriginal']
        if article['relatedPosts']:
            content += config['feed']['item']['relatedPostPrependHtml']
            recommendArticles = []
            for relatedPost in article['relatedPosts'][:6]:
                if relatedPost:
                    content += '<li><a href="%s">%s</li>' % (base_url+relatedPost['slug'], relatedPost['name'])
                    recommendArticle = {'title': relatedPost['name'], 'url': base_url + relatedPost['slug'] + config['feed']['item']['utmSource']}
                    if relatedPost['heroImage'] is not None:
                        recommendArticle['thumbnail'] = relatedPost['heroImage']['urlOriginal']
                    recommendArticles.append(recommendArticle)
            content += "</ul>"
        item['contents'] = {'text':{'content': content}}
        if article['relatedPosts']:
            item['recommendArticles'] = {'article': recommendArticles}
        item['author'] = config['feed']['item']['author']
        item['sourceUrl'] = base_url + article['slug'] + config['feed']['item']['utmSource']      
        if article['tags']:
            tags = []
            for tag in article['tags']:
                if tag:
                    tags.append(tag['name'])
            item['tags'] = {'tag':tags}
        mainXML['article'].append(item)

    root = ET.Element('articles')
    recparse(root, mainXML)

    data = '''<?xml version="1.0" encoding="UTF-8" ?>
    %s
    ''' % ET.tostring(root, encoding="unicode")

    file_config = config['file']
    # The name for the new bucket
    bucket_name = file_config['gcsBucket']

    # rss folder path
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
