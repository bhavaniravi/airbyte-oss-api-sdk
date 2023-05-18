from api.destination import DestinationAPI, DestinationDefinitionAPI
import pytest

workspace_id = "2d51cc69-400c-4c65-9409-b52def56235f"
destination_definition_id = "25c5221d-dce2-4163-ade9-739ef790f503"  # for postgres
destination_id = "2d51cc69-400c-4c65-9409-b52def56235f"


# def test_list_destination_definitions(connection):
#     hook = DestinationDefinitionAPI(connection)
#     response = hook.list_destination_definitions(workspace_id=workspace_id)


def test_destination_definition(connection):
    hook = DestinationDefinitionAPI(connection)
    response = hook.get_destination_definition(
        workspace_id=workspace_id, destination_definition_id=destination_definition_id
    )


def test_create_destination(connection):
    hook = DestinationAPI(connection)
    response = hook.create_destination(
        destination_name="new_pg",
        workspace_id=workspace_id,
        destination_definition_id=destination_definition_id,
        # need all these parameters for postgres API to work
        params={
            "host": "localhost",
            "port": 5432,
            "username": "postgres",
            "password": "password",
            "database": "postgres",
            "schema": "public",
            "ssl_connection": False,
            "ssl_mode": {"mode": "disable"},
            "tunnel_method": {"tunnel_method": "NO_TUNNEL"},
            "jdbc_url_params": "",
        },
    )


# def test_get_destination(connection):
#     # there is a bug in this API, the data is returned even after deletion
#     hook = DestinationAPI(connection)
#     response = hook.get_destination(destination_id=destination_id)


# def test_delete_destination(connection):
#     hook = DestinationAPI(connection)
#     response = hook.delete_destination(destination_id=destination_id)
