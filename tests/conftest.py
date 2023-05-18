from api.core import AirbyteHook, AirbyteConnection
import pytest
from dotenv import load_dotenv
import os

load_dotenv()


@pytest.fixture(scope="module")
def connection():
    host = os.environ["AIRBYTE_HOST"]
    username = os.environ["AIRBYTE_USERNAME"]
    password = os.environ["AIRBYTE_PASSWORD"]
    port = os.environ["AIRBYTE_PORT"]
    token = os.getenv("AIRBYTE_API_KEY")
    return AirbyteConnection(host, username, password, port=port, token=token)
