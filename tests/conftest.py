from api.core import AirbyteHook, AirbyteConnection
import pytest
from dotenv import load_dotenv

load_dotenv()


@pytest.fixture(scope="module")
def connection():
    host = os.env["AIRBYTE_HOST"]
    username = os.env["AIRBYTE_USERNAME"]
    password = os.env["AIRBYTE_PASSWORD"]
    port = os.env["port"]
    token = os.getenv("AIRBYTE_API_KEY")
    return AirbyteConnection(host, username, password, port=port, token=token)
