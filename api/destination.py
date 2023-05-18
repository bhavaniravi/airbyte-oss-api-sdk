from api.core import AirbyteHook
from api.models.destinations import DestinationDefinition
from airbyte.models import shared
import dataclasses


class DestinationDefinitionAPI(AirbyteHook):
    def list_destination_definitions(self, workspace_id):
        response = self.run(
            method="POST",
            endpoint=f"api/{self.connection.api_version}/destination_definitions/list_for_workspace",
            headers=self.headers,
            json={"workspaceId": workspace_id},
        )

        if response.status_code == 200:
            destination_def = [
                dataclasses.asdict(DestinationDefinition.from_dict(destination))
                for destination in response.json()["destinationDefinitions"]
            ]
            return destination_def
        else:
            return response

    def get_destination_definition(self, workspace_id, destination_definition_id):
        response = self.run(
            method="POST",
            endpoint=f"api/{self.connection.api_version}/destination_definitions/get_for_workspace",
            headers=self.headers,
            json={
                "destinationDefinitionId": destination_definition_id,
                "workspaceId": workspace_id,
            },
        )

        if response.status_code == 200:
            return DestinationDefinition.from_dict(response.json())


class DestinationAPI(AirbyteHook):
    def create_destination(
        self, workspace_id, destination_name, destination_definition_id, params
    ):
        destination_def = DestinationDefinitionAPI(self.connection)
        destination_def = destination_def.get_destination_definition(
            destination_definition_id=destination_definition_id,
            workspace_id=workspace_id,
        )
        destination_type = destination_def.name.lower()
        params["destination_type"] = destination_def
        DestinationClass = getattr(shared, f"Destination{destination_type.title()}")
        destination = DestinationClass(**params)
        params = dataclasses.asdict(destination)
        # self.check_destination_connection(
        #     workspace_id, destination_name, destination_definition_id, params
        # )
        return self.run(
            method="POST",
            endpoint=f"api/{self.connection.api_version}/destinations/create",
            headers=self.headers,
            json={
                "workspaceId": workspace_id,
                "name": destination_name,
                "destinationDefinitionId": destination_definition_id,
                "connectionConfiguration": params,
            },
        )

    def get_destination(self, destination_id):
        # there is a bug in this API, the destination_id is returned even after deletion
        # issue has been raised -

        return self.run(
            method="POST",
            endpoint=f"api/{self.connection.api_version}/destinations/get",
            headers=self.headers,
            json={"destinationId": destination_id},
        )

    def delete_destination(self, destination_id):
        return self.run(
            method="POST",
            endpoint=f"api/{self.connection.api_version}/destinations/delete",
            headers=self.headers,
            json={"destinationId": destination_id},
        )

    def list_destinations(self, workspace_id):
        response = self.run(
            method="POST",
            endpoint=f"api/{self.connection.api_version}/destinations/list_for_workspace",
            headers=self.headers,
            json={"workspaceId": workspace_id},
        )
        if response.status_code == 200:
            destinations = [
                dataclasses.asdict(Destination.from_dict(destination))
                for destination in response.json()["destinations"]
            ]
            return destinations
        else:
            return response
