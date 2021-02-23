import requests


def getAuthenticationCookie(graphqlEndpoint: str, user: str, secret: str):
    ses = requests.Session()
    ses.headers['User-Agent'] = 'Mozilla/5'

    r = ses.post(
        graphqlEndpoint,
        json={
            "operationName": "signin",
            "variables": {
                "identity": user,
                "secret": secret
            },
            "query": "mutation signin($identity: String, $secret: String) {\n  authenticate: authenticateUserWithPassword(email: $identity, password: $secret) {\n    item {\n      id\n      __typename\n    }\n    __typename\n  }\n}\n"
        }
    )

    return f"keystone.sid={ses.cookies.get('keystone.sid')}"
