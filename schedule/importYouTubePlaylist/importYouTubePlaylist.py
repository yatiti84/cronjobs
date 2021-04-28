from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from mergedeep import merge, Strategy
import argparse
import json
import os
import requests
import sys
import urllib.request
import yaml

CONFIG_KEY = 'config'
GRAPHQL_CMS_CONFIG_KEY = 'graphqlCMS'
PLAYLIST_IDS_KEY = 'playlistIds'
YOUTUBE_CREDENTIAL_KEY = 'youtubeCredential'

__defaultConfig = {
    'ytrelayEndpoints': {
        'playlistItems': 'http://yt-relay.default.svc.cluster.local/youtube/v3/playlistItems',
    },
    'converTextToDraftApiEndpoint': 'https://api.mirrormedia.mg/converttext',
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


def convertTextToDraft(config: dict, s: str) -> tuple:
    '''convertTextToDraft converts a string to a tuple of strings (draft, html, apiData)
    '''
    data = s.encode('utf-8')
    req = urllib.request.Request(config['converTextToDraftApiEndpoint'])
    req.add_header('Content-Type', 'text/plain')
    req.add_header('Accept', 'Application/json')
    with urllib.request.urlopen(req, data) as f:
        j = json.loads(f.read().decode('utf-8'))

    return (j['draft'], j['html'], j['apiData'])


# TODO excape the text
__mutationCreateVideoPostExample = '''
mutation {
  createPost(data:{
    slug: "4QePrv24TBU",
    state: draft,
    name:"ずっと真夜中でいいのに。『勘冴えて悔しいわ』MV (ZUTOMAYO - Kansaete Kuyashiiwa)",
    style: videoNews,
    brief: "チャンネル100万登録記念で制作したスピンオフ的MV\n『勘冴えて悔しいわ』\n(ZUTOMAYO - Kansaete Kuyashiiwa)\nLyrics & Vocal - ACAね\nMusic - ACAね, ラムシーニ\nArrangement - ラムシーニ, 100回嘔吐, ZTMY\nPiano - Jun☆Murayama\nDrums - Yoshihiro Kawamura\nBass - Ryosuke Nikamoto\nGuitar - Takayuki \"Kojiro\" Sasaki\nRec & Mix Engineer - Toru Matake\nMastering Engineer - Takeo Kira \nSound Direction - Kohei Matsumoto\n\nMV - sakiyama\n\n【ずっと真夜中でいいのに。 'ZUTOMAYO' 'ZTMY'】\n\"ACAね\"Twitter：https://twitter.com/zutomayo​\nOfficial Twitter : https://twitter.com/zutomayo_staff"
    source: "yt",
    heroVideo: {
      create:{state: draft, youtubeUrl: "https://www.youtube.com/watch?v=4QePrv24TBU",
          name: "ずっと真夜中でいいのに。『勘冴えて悔しいわ』MV (ZUTOMAYO - Kansaete Kuyashiiwa)", thumbnail: "https://i.ytimg.com/vi_webp/4QePrv24TBU/maxresdefault.webp?v=606187f8"}
    }
  }){
    id
    }
  }
}
'''


def main(config: dict = None, configGraphQL: dict = None, playlistIds: list = None):
    ''' Import YouTube Channel program starts here '''
    print(f'{__file__} is executing...')

    # merge option to the default configs
    config = merge({}, __defaultConfig, config,
                   strategy=Strategy.TYPESAFE_REPLACE)
    config_graphql = merge({}, __defaultgraphqlCmsConfig, configGraphQL,
                           strategy=Strategy.TYPESAFE_REPLACE)
    ytrelayPlaylistItemsAPI = config['ytrelayEndpoints']['playlistItems']

    # CMS get authentication token
    print(f'attempting to log in as CMS user:{configGraphQL["username"]}')

    # Authenticate through GraphQL

    gqlEndpoint = configGraphQL['apiEndpoint']
    gqlTransport = RequestsHTTPTransport(
        url=gqlEndpoint,
        use_json=True,
        headers={
            "Content-type": "application/json",
        },
        verify=True,
        retries=3,
    )
    gqlClient = Client(
        transport=gqlTransport,
        fetch_schema_from_transport=False,
    )
    qglMutationAuthenticateGetToken = '''
    mutation {
        authenticate: authenticateUserWithPassword(email: "%s", password: "%s") {
            token
        }
    }
    '''
    mutation = gql(qglMutationAuthenticateGetToken %
                   (configGraphQL['username'], configGraphQL['password']))

    token = gqlClient.execute(mutation)['authenticate']['token']
    print(
        f'{os.path.basename(__file__)} has authenticated as {configGraphQL["username"]}')

    gqlTransportWithToken = RequestsHTTPTransport(
        url=gqlEndpoint,
        use_json=True,
        headers={
            "Content-type": "application/json",
            'Authorization': f'Bearer {token}'
        },
        verify=True,
        retries=3,
    )

    gqlAuthenticatedClient = Client(
        transport=gqlTransportWithToken,
        fetch_schema_from_transport=False,
    )

    for playlistId in playlistIds:
        params = dict(
            part='snippet',
            maxResults=3,
            playlistId=playlistId,
            fields='items(id, snippet/title,snippet/description,snippet/thumbnails/maxres/url,snippet/resourceId/videoId)'
        )
        resp = requests.get(url=ytrelayPlaylistItemsAPI, params=params)
        data = resp.json()

        # check videos' existence in CMS
        # print(data)
        itemsID = [item['snippet']['resourceId']['videoId']
                   for item in data['items']]

        # format query array
        queryConditions = ','.join(
            ['{url_ends_with: ' + '"' + id + '"}' for id in itemsID])
        query = gql(__queryExistingVideosTemplate % queryConditions)
        existingVideos = gqlAuthenticatedClient.execute(query)['allVideos']
        videos = [{
            'id': id,
            'isInCMS': id in existingVideos,
        } for id in itemsID]
        # print(videos)
        # save new video to CMS
        for video in videos:
            if video['isInCMS']:
                print(f'Video({video["id"]}) is in CMS. Skip it.')
                continue
            print('create videos and posts in cms')


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Process configuration of importYouTubeChannel')
    parser.add_argument('-c', '--config', dest=CONFIG_KEY,
                        help='config file for importYouTubeChannel', metavar='FILE', type=str)
    parser.add_argument('-g', '--config-graphql', dest=GRAPHQL_CMS_CONFIG_KEY,
                        help='graphql config file for importYouTubeChannel', metavar='FILE', type=str, required=True)
    parser.add_argument('-p', '--playlist-ids', dest=PLAYLIST_IDS_KEY,
                        help='playlist ids, seperated by comma', metavar='PLIufxCyJpxOx4fCTTNcC7XCVZgY8MYQT5,PL1jBQxu5EklfCQ6rC-UzScji5zvNOBElG', type=str, required=True)

    args = parser.parse_args()

    with open(getattr(args, CONFIG_KEY), 'r') as stream:
        config = yaml.safe_load(stream)
    with open(getattr(args, GRAPHQL_CMS_CONFIG_KEY), 'r') as stream:
        configGraphQL = yaml.safe_load(stream)
    playlistIds = getattr(args, PLAYLIST_IDS_KEY).split(',')

    main(config, configGraphQL, playlistIds)
