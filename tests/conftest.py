from api.core import AirbyteHook, AirbyteConnection
import pytest
from dotenv import load_dotenv

load_dotenv()


@pytest.fixture(scope="module")
def connection():
    return AirbyteConnection("http://localhost", "airbyte", "password", port=8000)
