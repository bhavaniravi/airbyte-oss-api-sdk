import dataclasses
import inspect
from typing import Optional
from api.models.base import BaseDataClass
from airbyte.utils import utils
from airbyte.models import shared
from dataclasses_json import Undefined, dataclass_json


class SourceDefinitionFactory:
    def __init__(self):
        self.source_definitions = {}

    def register(self, source_type, source_definition):
        self.source_definitions[source_type] = source_definition

    def get(self, source_type):
        source_definition = self.source_definitions.get(source_type)
        if not source_definition:
            raise ValueError(source_type)
        return source_definition


@dataclasses.dataclass
class SourceDefinition(BaseDataClass):
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
    source_def_id: str = dataclasses.field(
        metadata={
            "dataclasses_json": {
                "letter_case": utils.get_field_name("sourceDefinitionId")
            }
        }
    )
    source_type: Optional[str] = dataclasses.field(
        default=None,
        metadata={
            "dataclasses_json": {"letter_case": utils.get_field_name("sourceType")}
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

    max_seconds_between_messages: Optional[int] = dataclasses.field(
        default=None,
        metadata={
            "dataclasses_json": {
                "letter_case": utils.get_field_name("maxSecondsBetweenMessages")
            }
        },
    )

    def __post_init__(self):
        self.sourceType = self.docker_repository.split("/")[-1].split("-")[-1]

    def __str__(self):
        return f"{self.name} ({self.sourceType})"


@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclasses.dataclass(kw_only=True)
class SourceResponse(shared.SourceResponse):
    source_type: Optional[str] = dataclasses.field(
        default=None,
        metadata={
            "dataclasses_json": {"letter_case": utils.get_field_name("sourceType")}
        },
        init=False,
    )
