from api.core import AirbyteHook
from api.models.source_definitions import SourceDefinition
from airbyte.models import shared
import dataclasses


class SourceDefinitionAPI(AirbyteHook):
    def get_source_definition(self, source_definition_id):
        return self.run(
            method="POST",
            endpoint=f"api/{self.connection.api_version}/source_definition_specifications/get",
            headers=self.headers,
            json={"sourceDefinitionId": source_definition_id},
        )

    def list_sources_definitions(self, workspace_id):
        response = self.run(
            method="POST",
            endpoint=f"api/{self.connection.api_version}/source_definitions/list_for_workspace",
            headers=self.headers,
            json={"workspaceId": workspace_id},
        )

        if response.status_code == 200:
            source_def = [
                dataclasses.asdict(SourceDefinition.from_dict(source))
                for source in response.json()["sourceDefinitions"]
            ]
            return source_def
        else:
            return response

    def get_source_definition(self, source_definition_id) -> SourceDefinition:
        response = self.run(
            method="POST",
            endpoint=f"api/{self.connection.api_version}/source_definitions/get",
            headers=self.headers,
            json={"sourceDefinitionId": source_definition_id},
        )

        if response.status_code == 200:
            return SourceDefinition.from_dict(response.json())


class SourcesAPI(AirbyteHook):
    def get_source_schema(self, source_id, connection_id=None):
        return self.run(
            method="POST",
            endpoint=f"api/{self.connection.api_version}/sources/discover_schema",
            headers=self.headers,
            json={"sourceId": source_id, "connectionId": connection_id},
        )

    def check_source_connection(
        self,
        workspace_id,
        source_name,
        source_definition_id,
        params,
        validate_params=False,
    ):
        """_summary_

        Args:
            workspace_id (_type_): _description_
            source_name (_type_): _description_
            source_definition_id (_type_): _description_
            params (_type_): _description_
            validate_params (bool, optional): set validate params if used from outside of create_source

        Returns:
            _type_: _description_
        """
        if validate_params:
            source_def = SourceDefinitionAPI(self.connection)
            source_def = source_def.get_source_definition(
                source_definition_id=source_definition_id
            )
            source_type = source_def.name.lower()
            params["source_type"] = source_def
            SourceClass = getattr(shared, f"Source{source_type.title()}")
            source = SourceClass(**params)
            params = dataclasses.asdict(source)

        return self.run(
            method="POST",
            endpoint=f"api/{self.connection.api_version}/sources/check_connection",
            headers=self.headers,
            json={
                "workspaceId": workspace_id,
                "name": source_name,
                "sourceDefinitionId": source_definition_id,
                "connectionConfiguration": params,
            },
        )

    def create_source(self, workspace_id, source_name, source_definition_id, params):
        source_def = SourceDefinitionAPI(self.connection)
        source_def = source_def.get_source_definition(
            source_definition_id=source_definition_id
        )
        source_type = source_def.name.lower()
        params["source_type"] = source_def
        SourceClass = getattr(shared, f"Source{source_type.title()}")
        source = SourceClass(**params)
        params = dataclasses.asdict(source)
        self.check_source_connection(
            workspace_id, source_name, source_definition_id, params
        )
        return self.run(
            method="POST",
            endpoint=f"api/{self.connection.api_version}/sources/create",
            headers=self.headers,
            json={
                "workspaceId": workspace_id,
                "name": source_name,
                "sourceDefinitionId": source_definition_id,
                "connectionConfiguration": params,
            },
        )

    def get_source(self, source_id):
        # there is a bug in this API, the source_id is returned even after deletion
        return self.run(
            method="POST",
            endpoint=f"api/{self.connection.api_version}/sources/get",
            headers=self.headers,
            json={"sourceId": source_id},
        )

    def delete_source(self, source_id):
        response = self.run(
            method="POST",
            endpoint=f"api/{self.connection.api_version}/sources/delete",
            headers=self.headers,
            json={"sourceId": source_id},
        )
        print(response.text)
        return response
