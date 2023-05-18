from typing import Optional
from api.models.base import BaseDataClass
from dataclasses_json import Undefined, dataclass_json
from airbyte.utils import utils
import dataclasses
import inspect


class DestinationDefinitionFactory:
    def __init__(self):
        self.destination_definitions = {}

    def register(self, destination_type, destination_definition):
        self.destination_definitions[destination_type] = destination_definition

    def get(self, destination_type):
        destination_definition = self.destination_definitions.get(destination_type)
        if not destination_definition:
            raise ValueError(destination_type)
        return destination_definition


@dataclasses.dataclass
class DestinationDefinition(BaseDataClass):
    name: str = dataclasses.field(
        metadata={"dataclasses_json": {"letter_case": utils.get_field_name("name")}}
    )
    docker_repository: str = dataclasses.field(
        metadata={
            "dataclasses_json": {
                "letter_case": utils.get_field_name("dockerRepository")
            }
        }
    )
    docker_image_tag: str = dataclasses.field(
        metadata={
            "dataclasses_json": {"letter_case": utils.get_field_name("dockerImageTag")}
        }
    )
    documentation_url: str = dataclasses.field(
        metadata={
            "dataclasses_json": {
                "letter_case": utils.get_field_name("documentationUrl")
            }
        }
    )
    destination_definition_id: str = dataclasses.field(
        metadata={
            "dataclasses_json": {
                "letter_case": utils.get_field_name("destinationDefinitionId")
            }
        }
    )
    destination_type: Optional[str] = dataclasses.field(
        default=None,
        metadata={
            "dataclasses_json": {"letter_case": utils.get_field_name("destinationType")}
        },
    )
    protocol_version: Optional[str] = dataclasses.field(
        default=None,
        metadata={
            "dataclasses_json": {"letter_case": utils.get_field_name("protocolVersion")}
        },
    )
    release_stage: Optional[str] = dataclasses.field(
        default=None,
        metadata={
            "dataclasses_json": {"letter_case": utils.get_field_name("releaseStage")}
        },
    )

    supports_dbt: Optional[bool] = dataclasses.field(
        default=None,
        metadata={
            "dataclasses_json": {"letter_case": utils.get_field_name("supportsDbt")}
        },
    )
    supports_normalization: Optional[bool] = dataclasses.field(
        default=None,
        metadata={
            "dataclasses_json": {
                "letter_case": utils.get_field_name("supportsNormalization")
            }
        },
    )
    supports_normalization_fully: Optional[bool] = dataclasses.field(
        default=None,
        metadata={
            "dataclasses_json": {
                "letter_case": utils.get_field_name("supportsNormalizationFully")
            }
        },
    )
    supports_replication: Optional[bool] = dataclasses.field(
        default=None,
        metadata={
            "dataclasses_json": {
                "letter_case": utils.get_field_name("supportsReplication")
            }
        },
    )
    supports_sync_mode: Optional[bool] = dataclasses.field(
        default=None,
        metadata={
            "dataclasses_json": {
                "letter_case": utils.get_field_name("supportsSyncMode")
            }
        },
    )
    supports_tombstone: Optional[bool] = dataclasses.field(
        default=None,
        metadata={
            "dataclasses_json": {
                "letter_case": utils.get_field_name("supportsTombstone")
            }
        },
    )
    supports_truncate: Optional[bool] = dataclasses.field(
        default=None,
        metadata={
            "dataclasses_json": {
                "letter_case": utils.get_field_name("supportsTruncate")
            }
        },
    )
    supports_upsert: Optional[bool] = dataclasses.field(
        default=None,
        metadata={
            "dataclasses_json": {"letter_case": utils.get_field_name("supportsUpsert")}
        },
    )

    def __post_init__(self):
        self.destination_type = self.docker_repository.split("/")[-1].split("-")[-1]

    def __str__(self):
        return f"{self.name} ({self.destination_type})"
