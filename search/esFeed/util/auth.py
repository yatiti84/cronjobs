import requests
import configparser
import os

config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), "../configs/cron.cfg"))

user = config.get("GRAPHQL", "USER")
secret = config.get("GRAPHQL", "SECRET")
endpoint = config.get("GRAPHQL", "ENDPOINT")


def getAuthenticationCookie():
    ses = requests.Session()
    ses.headers['User-Agent'] = 'Mozilla/5'

    r = ses.post(
        endpoint,
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
