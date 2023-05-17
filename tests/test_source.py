from api.sources import SourcesAPI, SourceDefinitionAPI
import pytest

workspace_id = "2d51cc69-400c-4c65-9409-b52def56235f"
# connection_id = "c5bc8a5d-c9c4-4399-89f4-1d5dd34ef170"
# source_id = "cc9f8665-7953-451f-8679-289ac5b9aefa"
source_definition_id = "decd338e-5647-4c0b-adf4-da0e75f5a750"  # for postgres
source_id = "6875991a-f77a-4cd2-a537-18d045c5c52a"

# def test_source_definition(connection):
#     hook = SourceDefinitionAPI(connection)
#     response = hook.get_source_definition(source_definition_id=source_definition_id)
#     print (response.json())

# def test_source_schema(connection):
#     hook = SourcesAPI(connection)
#     response = hook.get_source_schema(source_id=source_id)
#     print (response.json())


# def test_list_source_definitions(connection):
#     hook = SourceDefinitionAPI(connection)
#     response = hook.list_sources_definitions(workspace_id=workspace_id)


# def test_create_source(connection):
#     hook = SourcesAPI(connection)
#     response = hook.create_source(
#         source_name="new_pg",
#         workspace_id=workspace_id,
#         source_definition_id="decd338e-5647-4c0b-adf4-da0e75f5a750",
#         params={
#             "host": "localhost",
#             "port": 5432,
#             "username": "postgres",
#             "password": "password",
#             "database": "postgres",
#             "schemas": ["public"],
#             "ssl_mode": {"mode": "disable"},
#             "tunnel_method": {"tunnel_method": "NO_TUNNEL"},
#             "replication_method": {"method": "Standard"},
#             "jdbc_url_params": "",
#         },
#     )


def test_get_source(connection):
    # there is a bug in this API, the source_id is returned even after deletion
    hook = SourcesAPI(connection)
    response = hook.get_source(source_id=source_id)
    print(response.json())


def test_delete_source(connection):
    hook = SourcesAPI(connection)
    response = hook.delete_source(source_id=source_id)
    print("------------------", "source_id", source_id)
