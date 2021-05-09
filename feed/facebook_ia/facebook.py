import hashlib
import json

from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from feedgen.feed import FeedGenerator
from feedgen import util
from dateutil import parser, tz
from datetime import datetime, timezone
from google.cloud import storage
import gzip

BASE_URL = 'https://dev.mnews.tw/story/'
bucket_name = "static-mnews-tw-dev"
rss_base = 'rss'
time_zone = tz.gettz("Asia/Taipei")
GAID = "UA-83609754-2" # Newly set for
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
    allPosts(where: {source: "tv",  state: published}, sortBy: publishTime_DESC, first: 75) {
        name
        slug
        briefApiData
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

def parse_item(item):
    name = item['name']
    publish_time = item['publishTime']  # Should be in ISO format
    guid = hashlib.sha224((BASE_URL + item['slug']).encode()).hexdigest()
    brief = item['brief']
    article_body = item[''] # Brief? Should I parse html?
    return f"""
    <item>
      {op_tracker}
      <title>{name}</title>
      <link>http://example.com/article.html</link>
      <guid>{guid}</guid>
      <pubDate>{publish_time}</pubDate>
      <author>Mr. Author</author>
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
    rss = fg.rss_str(pretty=False, encoding='UTF-8', xml_declaration=True)
    fg.rss_file('./facebook_ia_rss.xml')

    # From rss remplate export to string
    with open('./facebook_ia_rss.xml') as f:
        f.write(rss_string)


def gql_call():
    # global result
    query = gql(gql_post_template)
    result = gql_client.execute(query)
    return result


if __name__ == '__main__':
    # main()
    result = gql_call()
    print(result)
