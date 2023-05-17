from api import AirbyteHook, AirbyteConnection
import pytest


@pytest.fixture(autouse=True)
def connection():
    return AirbyteConnection("http://localhost", "airbyte", "password", port=8000)


connection_id = "c5bc8a5d-c9c4-4399-89f4-1d5dd34ef170"


def create_connection():
    pass


# def test_class_init():
#     hook = AirbyteHook(connection)

# def test_get_latest_job_id():

#     hook = AirbyteHook(connection)
#     response = hook.get_latest_job(connection_id)
#     assert isinstance(response.json()["job"]["id"], int)

# def test_cancel_job():
#     hook = AirbyteHook(connection)
#     response = hook.cancel_job(4)


def test_create_sync():
    print("hello...")
    hook = AirbyteHook(connection)
    response = hook.sync_connection(connection_id=connection_id)
    if response.status_code == 200:
        assert isinstance(response.json()["job"]["id"], int)
    elif response.status_code == 409:
        assert (
            response.json()["exceptionClassName"]
            == "io.airbyte.commons.server.errors.ValueConflictKnownException"
        )


# def test_create_connection():
#     pass

# def test_get_connection():
#     pass

# def test_create_source():
#     pass

# def test_get_source():
#     pass

# def test_create_destination():
#     pass

# def test_get_destination():
#     pass

# def test_create_sync():
#     pass

# def test_get_job():
#     pass
