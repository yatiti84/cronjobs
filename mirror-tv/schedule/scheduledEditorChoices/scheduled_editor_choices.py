from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport

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

# TODO email and password must be provided from external source
# Authenticate through GraphQL
__qgl_mutation_authenticate_get_token__ = '''
mutation {
  authenticate: authenticateUserWithPassword(email: "%s", password: "%s") {
    token
  }
}
'''


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
editor_choices = __gql_client__.execute(query)['allEditorChoices']


def get_updated_state_value(state):
    states_waterfall = ['scheduled', 'published', 'draft']

    # update the state value according to the original state, only scheduled and published will not cause an exception
    # if state is not scheduled and published, then it exception happens and we do not update it
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
