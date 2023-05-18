from api.core import AirbyteHook
from api.models import connections
from airbyte.models import shared, operations
from airbyte.utils import utils
from typing import Optional
import dataclasses


class ConnectionsAPI(AirbyteHook):
    def create_connection(self, params):
        conn = connections.ConnectionRequest.from_dict(params)
        params = conn.to_dict()
        print(params)
        response = self.run(
            method="POST",
            endpoint=f"api/{self.connection.api_version}/connections/create",
            headers=self.headers,
            json=params,
        )
        print(response.json())
        return connections.ConnectionResponse.from_dict(response.json())
