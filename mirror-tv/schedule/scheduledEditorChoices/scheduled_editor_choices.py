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

# To query the EditorChoices in state of published and scheduled, so it can tolling update their state
__qgl_query_editor_choices_to_modify__ = '''
{
  allEditorChoices(where: {OR: [{state: published}, {state: scheduled}]}) {
    id
    state
  }
}
'''

# TODO email and password must be provided from external source
# Authenticate through GraphQL
__qgl_mutation_authenticate_get_token__ = '''
mutation {
  authenticate: authenticateUserWithPassword(email: "%s", password: "%s") {
    token
  }
}
'''

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
