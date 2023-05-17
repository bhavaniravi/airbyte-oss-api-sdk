import dataclasses
import inspect

@dataclasses.dataclass
class BaseDataClass:
    @classmethod
    def from_dict(cls, env): 
        params = {
            k: v for k, v in env.items() 
            if k in inspect.signature(cls).parameters
        }     
        return cls(**params)

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
    

    def __post_init__(self):
        self.source_type = self.dockerRepository.split("/")[-1].split("-")[-1]


    def __str__(self):
        return f"{self.name} ({self.sourceType})"


# create a dict class with mapping sourceType and SourceClass

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