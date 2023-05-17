from api.models.base import BaseDataClass
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
    name: str
    dockerRepository: str
    dockerImageTag: str
    documentationUrl: str
    destinationDefinitionId: str
    destinationType: str = None
    protocolVersion: str = None
    releaseStage: str = None
    supportsDbt: bool = None
    supportsNormalization: bool = None
    supportsNormalizationFully: bool = None
    supportsReplication: bool = None
    supportsSyncMode: bool = None
    supportsTombstone: bool = None
    supportsTruncate: bool = None
    supportsUpsert: bool = None

    def __post_init__(self):
        self.destinationType = self.dockerRepository.split("/")[-1].split("-")[-1]

    def __str__(self):
        return f"{self.name} ({self.destinationType})"
