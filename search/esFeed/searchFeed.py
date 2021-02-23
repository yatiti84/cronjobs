from __future__ import print_function
import dateutil.parser
import datetime
from bson import json_util
import json
import time
from elasticsearch import Elasticsearch, NotFoundError
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from esFeed.util import auth
import configparser
import sys
import os
import re
import ast
import math


# get configuration from argv
config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), "../configs/cron.cfg"))

gqlEndpoint = config.get("GRAPHQL", "ENDPOINT")
esEndpoint = config.get("ELASTICSEARCH", "ENDPOINT")
postsIndex = config.get("SEARCHFEED", "POSTS_INDEX")
metaIndex = config.get("SEARCHFEED", "META_INDEX")
unitDays = ast.literal_eval(config.get("SEARCHFEED", "UNIT_DAYS"))
savedFields = ast.literal_eval(config.get("SEARCHFEED", "SAVED_FIELDS"))

# prepare instances
es = Elasticsearch(esEndpoint)

# set authentication cookie
transport = AIOHTTPTransport(
    url=gqlEndpoint,
    headers={
        "Cookie": auth.getAuthenticationCookie()
    }
)
client = Client(transport=transport, fetch_schema_from_transport=True)


def main():
    ''' Search-feed program starts here '''
    # create search-feed indices if not exist
    createSearchFeedIndices()

    initDt = getLastUpdateDatetime()
    print("\n[SearchFeed] starts to update docs modified after `{dt}` to es at {current}:".format(
        dt=initDt, current=datetime.datetime.now()))

    if len(sys.argv) == 2:
        beforeDays = float(sys.argv[1])
        total = 0
        for i in range(int(math.ceil(beforeDays/unitDays))):
            remainingDays = ((beforeDays - i * unitDays) % unitDays,
                             unitDays)[(beforeDays - i * unitDays) / unitDays >= 1]
            startDt = initDt + datetime.timedelta(days=(i * unitDays))
            endDt = startDt + datetime.timedelta(days=remainingDays)

            fetchedPosts = getPostsUpdatedBetween(startDt, endDt)
            processSearchFeed(fetchedPosts)
            total += len(fetchedPosts)
        printFinMessages(total)
    else:
        fetchedPosts = getPostsUpdatedBetween(initDt)
        processSearchFeed(fetchedPosts)
        printFinMessages(len(fetchedPosts))


def printFinMessages(fetchedPostsCount):
    print(
        "Search-feed done at {current}!".format(current=datetime.datetime.now()))
    print("{count} docs handled.".format(count=fetchedPostsCount))


def processSearchFeed(fetchedPosts):
    for post in fetchedPosts:
        cleanedPost = clean(post)
        updateElasticsearch(cleanedPost)
    if len(fetchedPosts) > 0:
        saveLastUpdateDatetime(dateutil.parser.isoparse(
            fetchedPosts[-1]["updatedAt"]))


def getPostsUpdatedBetween(startDt, endDt=None):
    timeRange = "{{updatedAt_gt: \"{}\"}}".format(startDt.isoformat())
    if endDt:
        timeRange = timeRange + \
            ", {{updatedAt_lte: \"{}\"}}".format(endDt.isoformat())

    getScheduledItemsQuery = gql(
        """
        query {
            allPosts(where: { AND: [ { OR: [{isAdvertised: null}, {isAdvertised: false}]}, %s ] }) {
                id
                slug
                title
                subtitle
                state
                publishTime
                categories {
                    title
                    ogTitle
                    ogDescription
                }
                writers {
                    name
                }
                photographers {
                    name
                }
                cameraOperators {
                    name
                }
                designers {
                    name
                }
                engineers {
                    name
                }
                vocals {
                    name
                }
                otherbyline
                heroVideo {
                    title
                    description
                }
                heroImage {
                    title
                    keywords
                    urlMobileSized
                }
                heroCaption
                style
                brief
                content
                topics {
                    title
                    subtitle
                }
                tags {
                    name
                    ogTitle
                    ogDescription
                }
                audio {
                    title
                }
                ogTitle
                ogDescription
                ogImage {
                    title
                    keywords
                }
                updatedAt
            }
        }
        """ % (timeRange)
    )
    return client.execute(getScheduledItemsQuery)["allPosts"]


def clean(post):
    cleanedPost = {}
    _id = post["id"]
    state = post["state"]
    for field in savedFields:
        cleanedPost[field] = post[field]
    if post["brief"] is not None:
        cleanedPost["brief"] = json.loads(post["brief"])["html"]
    if post["content"] is not None:
        cleanedPost["content"] = json.loads(post["content"])['html']
    return {"_id": _id, "state": state, "doc": cleanedPost}


def updateElasticsearch(cleanedPost):
    _id = cleanedPost["_id"]
    state = cleanedPost["state"]
    doc = cleanedPost["doc"]
    title = doc["title"]

    if state == "published":
        es.update(index=postsIndex, doc_type="_doc", id=_id,
                  body={"doc": doc, "doc_as_upsert": True})
        print(
            "[SearchFeed] insert/update {id}: {title}".format(id=str(_id), title=title))
    else:
        es.delete(index=postsIndex, doc_type="_doc", id=_id, ignore=[400, 404])
        print("[SearchFeed] delete {id}: {title}".format(
            id=str(_id), title=title))


def getLastUpdateDatetime():
    try:
        if len(sys.argv) == 2:
            beforeDays = float(sys.argv[1])
            print("\n[SearchFeed] recieved a time param. Will fetch posts started from `{beforeDays}` days ago!".format(
                beforeDays=beforeDays))
            return datetime.datetime.now() - datetime.timedelta(days=beforeDays)

        meta = es.get(index=metaIndex, doc_type="_doc", id="meta")
        ts = int(meta['_source']['ts'])
        return datetime.datetime.fromtimestamp(ts / 1000) + datetime.timedelta(milliseconds=ts % 1000)
    except NotFoundError:
        return datetime.datetime.now() - datetime.timedelta(minutes=5)


def saveLastUpdateDatetime(dt):
    milliseconds = int(time.mktime(dt.utctimetuple())
                       * 1000 + dt.microsecond / 1000.0)
    es.index(index=metaIndex, doc_type="_doc",
             id="meta", body={"ts": str(milliseconds)})


def createSearchFeedIndices():
    es.indices.create(index=postsIndex, ignore=400, body={
        "mappings": {
            "_doc": {
                "properties": {
                    "publishTime": {
                        "type": "date",
                        "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd HH:mm:ss.SSSSSS",
                    }
                }
            }
        }
    })
    es.indices.create(index=metaIndex, ignore=400)


# define some helpers for debug use
def pp(obj):
    print(json_util.dumps(obj, indent=2))


if __name__ == '__main__':
    main()
