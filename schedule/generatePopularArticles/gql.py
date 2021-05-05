import requests


def CDN(fileHostDomainRule: dict, url: str):
    for key in fileHostDomainRule.keys():
        url = url.replace(key, fileHostDomainRule[key], 1)
    return url


def gql_query_from_slugs(configGraphQL: dict, fileHostDomainRule: dict, slugs: list) -> list:
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

    r = requests.post(configGraphQL['apiEndpoint'], json={"query": gql_query})

    data = r.json()['data']['allPosts']
    for item in data:
        if item['heroImage']:
            item['heroImage']['urlMobileSized'] = CDN(fileHostDomainRule,
                                                      item['heroImage']['urlMobileSized'])
            item['heroImage']['urlTinySized'] = CDN(fileHostDomainRule,
                                                    item['heroImage']['urlTinySized'])

    return data
