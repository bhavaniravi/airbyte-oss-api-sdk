import dataclasses
import inspect
from api.models.base import BaseDataClass


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
    name: str
    dockerRepository: str
    dockerImageTag: str
    documentationUrl: str
    sourceDefinitionId: str
    sourceType: str = None
    protocolVersion: str = None
    releaseStage: str = None
    maxSecondsBetweenMessages: int = None
    sourceFactory: SourceDefinitionFactory = None

    def __post_init__(self):
        self.sourceType = self.dockerRepository.split("/")[-1].split("-")[-1]

    def __str__(self):
        return f"{self.name} ({self.sourceType})"


# create a dict class with mapping sourceType and SourceClass