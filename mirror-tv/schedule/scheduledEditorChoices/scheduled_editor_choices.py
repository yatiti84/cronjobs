from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from environs import Env
from google.cloud import secretmanager

# TODO move it to commandline arguments
__cms_graphql_endpoint__ = 'http://mirror-tv-graphql.default.svc.cluster.local/admin/api'

__gql_transport__ = RequestsHTTPTransport(
    url=__cms_graphql_endpoint__,
    use_json=True,
    headers={
        "Content-type": "application/json",
    },
    verify=True,
    retries=3,
)

__gql_client__ = Client(
    transport=__gql_transport__,
    fetch_schema_from_transport=True,
)

env = Env()
env.read_env()  # read .env file, if it exists


__gcp_project_id__ = env('GCP_PROJECT_ID')


# Create the Secret Manager client.
__secretmanager_client__ = secretmanager.SecretManagerServiceClient()


def access_gcp_secret(project_id: str = __gcp_project_id__, secret_id: str = None, secret_version: str = 'latest') -> str:
    '''access_gcp_secret returns the secret on gcp'''

    if str == None:
        raise Exception('secret_id is not provided')

    # Build secret path a.k.a. secret name
    secret_path = f'projects/{project_id}/secrets/{secret_id}/versions/{secret_version}'

    # Access the secret version.
    response = __secretmanager_client__.access_secret_version(
        request={'name': secret_path})

    # Print the secret payload.
    #
    # WARNING: Do not print the secret in a production environment - this
    # snippet is showing how to access the secret material.
    return response.payload.data.decode('UTF-8')


# Authenticate through GraphQL
__qgl_mutation_authenticate_get_token__ = '''
mutation {
  authenticate: authenticateUserWithPassword(email: "%s", password: "%s") {
    token
  }
}
'''

__gcp_secert_id_cronjobs_username__ = env('GCP_SECERT_ID_CRONJOBS_USERNAME')
__gcp_secert_version_cronjobs_username__ = env(
    'GCP_SECERT_VERSION_CRONJOBS_USERNAME')
__gcp_secret_id_cronjob_password__ = env('GCP_SECERT_ID_CRONJOBS_PASSWORD')
__gcp_secret_version_cronjobs_password__ = env(
    'GCP_SECERT_VERSION_CRONJOBS_PASSWORD')

__username__ = access_gcp_secret(
    secret_id=__gcp_secert_id_cronjobs_username__, secret_version=__gcp_secert_version_cronjobs_username__)
__password__ = access_gcp_secret(
    secret_id=__gcp_secret_id_cronjob_password__, secret_version=__gcp_secret_version_cronjobs_password__)

mutation = gql(__qgl_mutation_authenticate_get_token__ %
               (__username__, __password__))

__token__ = __gql_client__.execute(mutation)['authenticate']['token']
print(__token__)

__gql_transport_with_token__ = __gql_transport__.headers.update(
    {'Authentication': f'Bearer {__token__}'})

__gql_authenticated_client__ = __gql_client__.transport = __gql_transport_with_token__


# To query the EditorChoices in state of published and scheduled, so it can tolling update their state
__qgl_query_editor_choices_to_modify__ = '''
{
    allEditorChoices(where: {OR: [{state: published}, {state: scheduled}]}) {
        id
        state
    }
}
'''

query = gql(__qgl_query_editor_choices_to_modify__)
editor_choices = __gql_authenticated_client__.execute(query)[
    'allEditorChoices']


def get_updated_state_value(state: str = 'draft') -> str:
    states_waterfall = ['scheduled', 'published', 'draft']

    # update the state value according to the original state, only scheduled and published will not cause an exception
    # if state is not scheduled and published, then an exception happens and we do not update it
    try:
        return states_waterfall[states_waterfall.index(state) + 1]
    except:
        return state


new_data_str = str([{'id': editor_choice['id'], 'state': get_updated_state_value(editor_choice['state'])}
                    for editor_choice in editor_choices])

# To update EditorChoices, data should be an array of objects containing id, and data
__qgl_mutate_editor_choices_template__ = '''
mutation {
    updateEditorChoices(data: %s) {
        id
        state
    }
}
'''

# Unauthenticate user after finishing updating to protect the user. Cronjobs' unauthentication shouldn't interfere each other.
__qgl_mutate_unauthenticate_user__ = '''
__qgl_mutate_editor_choices_template__ = mutation {
    unauthenticate: unauthenticateUser {
        success
    }
}
'''
