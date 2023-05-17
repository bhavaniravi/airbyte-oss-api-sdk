from pydantic import BaseModel, Field


class SourceDefinition(BaseModel):
    name: str = Field(...)
    dockerRepository: str = Field(...)
    dockerImageTag: str = Field(...)
    documentationUrl: str = Field(...)
    sourceDefinitionId: str = Field(...)
    sourceType: str = Field(None)
    protocolVersion: str = Field(None)
    releaseStage: str = Field(None)
    maxSecondsBetweenMessages: int = Field(None)
