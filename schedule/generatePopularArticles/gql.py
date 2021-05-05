import requests
from tvGA import API_URL
from typing import List


def CDN(url):
    url = url.replace('https://storage.googleapis.com/mirrormedia-files', 'https://www.mirrormedia.mg')
    url = url.replace('https://storage.googleapis.com/static-mnews-tw-dev', 'https://dev.mnews.tw')
    url = url.replace('https://storage.googleapis.com/mirror-tv-file', 'https://www.mnews.tw')
    return url

def gql_query_from_slugs(slugs: List) -> List:
    slug_cond = ','.join([f'{{slug: "{slug}" }}' for slug in slugs])

    gql_query = f"""
    query{{ 
       allPosts(where: {{OR:[ {slug_cond} ] }}){{
         id
         publishTime
         heroImage {{urlMobileSized, urlTinySized}}
         slug
         name
         
     }}
    }}
    """
    # Learn this from playground

    r = requests.post(API_URL, json={"query": gql_query})

    data = r.json()['data']['allPosts']
    for item in data:
        if item['heroImage']:
            item['heroImage']['urlMobileSized'] = CDN(item['heroImage']['urlMobileSized'])
            item['heroImage']['urlTinySized'] = CDN(item['heroImage']['urlTinySized'])

    return data


