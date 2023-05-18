from api.connections import ConnectionsAPI
import pytest
import json


def test_create_source(connection):
    hook = ConnectionsAPI(connection)
    with open("tests/data/connection/connection_postgres_postgres.json") as f:
        params = json.load(f)
        response = hook.create_connection(params)

    assert response.connection_id is not None
