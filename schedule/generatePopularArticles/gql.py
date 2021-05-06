import requests


def CDN(file_host_domain_rule: dict, url: str):
    for key in file_host_domain_rule.keys():
        url = url.replace(key, file_host_domain_rule[key], 1)
    return url


def gql_query_from_slugs(config_graphql: dict, file_host_domain_rule: dict, slugs: list) -> list:
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

    r = requests.post(config_graphql['apiEndpoint'], json={"query": gql_query})

    data = r.json()['data']['allPosts']
    for item in data:
        if item['heroImage']:
            item['heroImage']['urlMobileSized'] = CDN(file_host_domain_rule,
                                                      item['heroImage']['urlMobileSized'])
            item['heroImage']['urlTinySized'] = CDN(file_host_domain_rule,
                                                    item['heroImage']['urlTinySized'])

    return data
