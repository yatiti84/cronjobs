from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from environs import Env
from google.cloud import secretmanager
import os
import sys

'''
For scheduled_editor_choices to run, a CMS bot user is required for it to mutate Editor Choices.

To authenticate as the bot user, the following environmental variables are required, mainly to access GCP secret:

GCP_PROJECT_ID
GCP_SECERT_ID_CRONJOBS_USERNAME
GCP_SECERT_VERSION_CRONJOBS_USERNAME
GCP_SECERT_ID_CRONJOBS_PASSWORD
GCP_SECERT_VERSION_CRONJOBS_PASSWORD

The names are self explained.
'''


def help():
    '''Print usage'''
    print(f'''
Usage:
{os.path.basename(__file__)} "cms graphql endpoint"
    ''')


if len(sys.argv) != 2:
    print('The number of arguments is wrong')
    help()
    exit(1)

__cms_graphql_endpoint__ = sys.argv[1]

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
    fetch_schema_from_transport=False,
)

env = Env()
env.read_env()  # read .env file, if it exists

# Create the Secret Manager client.
__secretmanager_client__ = secretmanager.SecretManagerServiceClient()

__gcp_project_id__ = env('GCP_PROJECT_ID')


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

print(f'{os.path.basename(__file__)} has authenticated as {__username__}')

__gql_transport_with_token__ = RequestsHTTPTransport(
    url=__cms_graphql_endpoint__,
    use_json=True,
    headers={
        "Content-type": "application/json",
        'Authorization': f'Bearer {__token__}'
    },
    verify=True,
    retries=3,
)

__gql_authenticated_client__ = Client(
    transport=__gql_transport_with_token__,
    fetch_schema_from_transport=False,
)


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

if len(editor_choices) == 0:
    print('There is nothing to be updated. Exit now...')
    exit(0)

print(
    f'These editor choices are about to be updated: {editor_choices}')


def get_updated_state_value(state: str = 'draft') -> str:
    states_waterfall = ['scheduled', 'published', 'draft']

    # update the state value according to the original state, only scheduled and published will not cause an exception
    # if state is not scheduled and published, then an exception happens and we do not update it
    try:
        return states_waterfall[states_waterfall.index(state) + 1]
    except:
        return state


# To update EditorChoices, data should be an array of objects containing id and data

new_data_list = ['{id: "%s", data:{state: %s}}' % (
    editor_choice["id"], get_updated_state_value(editor_choice["state"])) for editor_choice in editor_choices]
new_data_str = '[' + ','.join(new_data_list) + ']'


print(
    f'The editor choices is going to be updated as: {new_data_str}')

__qgl_mutate_editor_choices_template__ = '''
mutation {
    updateEditorChoices(data: %s) {
        id
        state
    }
}
'''

mutation = gql(__qgl_mutate_editor_choices_template__ % new_data_str)
updateEditorChoices = __gql_authenticated_client__.execute(mutation)

print(f'EditorChoices are updated as:{updateEditorChoices}')

# Unauthenticate user after finishing updating to protect the user. Cronjobs' unauthentication shouldn't interfere each other.
__qgl_mutate_unauthenticate_user__ = '''
mutation {
    unauthenticate: unauthenticateUser {
        success
    }
}
'''

mutation = gql(__qgl_mutate_unauthenticate_user__)
unauthenticate = __gql_authenticated_client__.execute(mutation)[
    'unauthenticate']

if unauthenticate['success'] == True:
    print(f'{os.path.basename(__file__)} has unauthenticated as {__username__}')
else:
    print(f'{__username__} failed to unauthenticate')
