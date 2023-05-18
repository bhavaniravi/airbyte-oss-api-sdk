from api.sources import SourcesAPI, SourceDefinitionAPI
import pytest

workspace_id = "2d51cc69-400c-4c65-9409-b52def56235f"
source_definition_id = "decd338e-5647-4c0b-adf4-da0e75f5a750"  # for postgres
source_id = "6875991a-f77a-4cd2-a537-18d045c5c52a"


def test_source_definition(connection):
    hook = SourceDefinitionAPI(connection)
    response = hook.get_source_definition(source_definition_id=source_definition_id)


def test_source_schema(connection):
    hook = SourcesAPI(connection)
    response = hook.get_source_schema(source_id=source_id)


def test_list_source_definitions(connection):
    hook = SourceDefinitionAPI(connection)
    response = hook.list_sources_definitions(workspace_id=workspace_id)

# get the list
# filter by name = hubspot
# schema of request params
# use schema to construct request
# pass it to create_source


def test_create_source(connection):
    hook = SourcesAPI(connection)
    response = hook.create_source(
        source_name="new_pg",
        workspace_id=workspace_id,
        source_definition_id=source_definition_id,
        # need all these parameters for postgres API to work
        params={
            "host": "localhost",
            "port": 5432,
            "username": "postgres",
            "password": "password",
            "database": "postgres",
            "schemas": ["public"],
            "ssl_mode": {"mode": "disable"},
            "tunnel_method": {"tunnel_method": "NO_TUNNEL"},
            "replication_method": {"method": "Standard"},
            "jdbc_url_params": "",
        },
    )

    assert response.source_response.source_id is not None


def test_get_source(connection):
    # there is a bug in this API, the data is returned even after deletion
    hook = SourcesAPI(connection)
    response = hook.get_source(source_id=source_id)
    assert response.source_response.source_id is not None


def test_delete_source(connection):
    hook = SourcesAPI(connection)
    response = hook.delete_source(source_id=source_id)
    assert response.status_code == 204
