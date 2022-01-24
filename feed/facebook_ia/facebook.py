import hashlib
from lxml.etree import HTML
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from dateutil import tz
from datetime import datetime, timezone
from google.cloud import storage
import gzip

BASE_URL = 'https://dev.mnews.tw/story/'
bucket_name = "static-mnews-tw-dev"
rss_base = 'rss'
time_zone = tz.gettz("Asia/Taipei")
GAID = "UA-83609754-2"
GQL_API = 'https://graphql-external-dev.mnews.tw/admin/api'

gql_transport = RequestsHTTPTransport(
    url=GQL_API,
    use_json=True,
    headers={
        "Content-type": "application/json",
    },
    verify=True,
    retries=3,
)

gql_client = Client(
    transport=gql_transport,
    fetch_schema_from_transport=True,
)

gql_post_template = '''
{
    allPosts(where: {source: "tv",  state: published, }, sortBy: publishTime_DESC, first: 75) {
        name
        slug
        briefApiData
        briefHtml
        heroImage {
            urlOriginal
        }  
        publishTime
        updatedAt
    }
}
'''

tracking_code = """
<script>
var _comscore = _comscore || [];

_comscore.push({ c1: "2", c2: "24318560" });

(function() {

var s = document.createElement("script"), el = document.getElementsByTagName("script")[0];

s.async = true; s.src = "https://sb.scorecardresearch.com/cs/24318560/beacon.js";

el.parentNode.insertBefore(s, el);

})();
</script>
"""

#  figure class=“op-tracker”
op_tracker = f"""
<figure class="op-tracker">
    <iframe hidden>
        {tracking_code}
    </iframe>
</figure>
"""

def gql_call():
    query = gql(gql_post_template)
    result = gql_client.execute(query)
    return result

def upload_data(bucket_name: str, data: bytes, content_type: str, destination_blob_name: str):
    '''Uploads a file to the bucket.'''
    # bucket_name = 'your-bucket-name'
    # data = 'storage-object-content'
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.content_encoding = 'gzip'
    blob.upload_from_string(
        data=gzip.compress(data=data, compresslevel=9), content_type=content_type, client=storage_client)
    blob.content_language = 'zh'
    blob.cache_control = 'max-age=300,public'
    blob.patch()

def parse_item(item):
    name = item['name']
    publish_time = item['publishTime']  # Should be in ISO format
    guid = hashlib.sha224((BASE_URL + item['slug']).encode()).hexdigest()
    brief = item['briefApiData']
    article_body = parse_html(item)
    return f"""
    <item>
      {op_tracker}
      <title>{name}</title>
      <link>http://example.com/article.html</link>
      <guid>{guid}</guid>
      <pubDate>{publish_time}</pubDate>
      <description>{brief}</description>
      <content:encoded>
        <![CDATA[
        <!doctype html>
        <html lang="en" prefix="op: http://media.facebook.com/op#">
          <head>
            <meta charset="utf-8">
            <link rel="canonical" href="http://example.com/article.html">
            <meta property="op:markup_version" content="v1.0">
          </head>
          <body>
            <article>
              <header>
                <!— Article header goes here -->
              </header>
              {article_body}
              <footer>
                <!— Article footer goes here -->
              </footer>
            </article>
          </body>
        </html>
        ]]>
      </content:encoded>
    </item>
    """

def parse_html(item):
    if item.get('briefHtml',''):
        html = HTML(item['briefHtml'])
        if html.xpath('//text()'):
            article_body = html.xpath('//text()')[0]
        else:
            article_body = ''
    else:
        article_body = ''
    return article_body


def main():
    # global result, item
    result = gql_call()
    article_items = '\n'.join([parse_item(item) for item in result['allPosts']])
    rss_string = f"""
    <rss version="2.0"
xmlns:content="http://purl.org/rss/1.0/modules/content/">
  <channel>
    <title>Facebook IA</title>
    <link>https://dev.mnews.tw</link>
    <description>
      News from MNEWS.
    </description>
    <language>zh-tw</language>
    <lastBuildDate>{datetime.now(timezone.utc).astimezone(time_zone)}</lastBuildDate>
    {article_items}
  </channel>
</rss>
    """
    upload_data(bucket_name=bucket_name, data=rss_string, content_type='application/xml',
        destination_blob_name=rss_base +'facebook_ia_rss.xml')

if __name__ == '__main__':
    main()
