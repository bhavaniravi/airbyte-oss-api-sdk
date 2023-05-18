from api.core import AirbyteHook
from api.models import sources
from airbyte.models import shared, operations
from airbyte.utils import utils
from typing import Optional
import dataclasses


class SourceDefinitionAPI(AirbyteHook):
    def get_source_definition(self, workspace_id, source_definition_id):
        return self.run(
            method="POST",
            endpoint=f"api/{self.connection.api_version}/source_definitions/get_for_workspace",
            headers=self.headers,
            json={
                "sourceDefinitionId": source_definition_id,
                "workspaceId": workspace_id,
            },
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
                dataclasses.asdict(sources.SourceDefinition.from_dict(source))
                for source in response.json()["sourceDefinitions"]
            ]
            return source_def
        else:
            return response

    def get_source_definition(self, source_definition_id) -> sources.SourceDefinition:
        response = self.run(
            method="POST",
            endpoint=f"api/{self.connection.api_version}/source_definitions/get",
            headers=self.headers,
            json={"sourceDefinitionId": source_definition_id},
        )

        if response.status_code == 200:
            return sources.SourceDefinition.from_dict(response.json())


class SourcesAPI(AirbyteHook):
    def get_source_schema(self, source_id, connection_id=None):
        return self.run(
            method="POST",
            endpoint=f"api/{self.connection.api_version}/sources/discover_schema",
            headers=self.headers,
            json={"sourceId": source_id, "connectionId": connection_id},
        )

    def create_source(
        self, workspace_id, source_name, source_definition_id, params
    ) -> operations.CreateSourceResponse:
        source_def = SourceDefinitionAPI(self.connection)
        source_def = source_def.get_source_definition(
            source_definition_id=source_definition_id
        )
        source_type = source_def.name.lower()
        params["source_type"] = source_type
        SourceClass = getattr(shared, f"Source{source_type.title()}")
        source = SourceClass(**params)
        params = dataclasses.asdict(source)
        http_res = self.run(
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
        content_type = http_res.headers.get("Content-Type")

        res = operations.CreateSourceResponse(
            status_code=http_res.status_code,
            content_type=content_type,
            raw_response=http_res,
        )

        if http_res.status_code == 200:
            if utils.match_content_type(content_type, "application/json"):
                out = sources.SourceResponse.from_dict(http_res.json())
                res.source_response = out

        return res

    def get_source(self, source_id) -> operations.GetSourceResponse:
        # there is a bug in this API, the source_id is returned even after deletion
        # issue has been raised - https://github.com/airbytehq/airbyte/issues/26182
        http_res = self.run(
            method="POST",
            endpoint=f"api/{self.connection.api_version}/sources/get",
            headers=self.headers,
            json={"sourceId": source_id},
        )

        content_type = http_res.headers.get("Content-Type")

        res = operations.GetSourceResponse(
            status_code=http_res.status_code,
            content_type=content_type,
            raw_response=http_res,
        )

        if http_res.status_code == 200:
            if utils.match_content_type(content_type, "application/json"):
                res.source_response = sources.SourceResponse.from_dict(http_res.json())

        return res

    def delete_source(self, source_id):
        response = self.run(
            method="POST",
            endpoint=f"api/{self.connection.api_version}/sources/delete",
            headers=self.headers,
            json={"sourceId": source_id},
        )
        return response
