from typing import List, Optional
from dataclasses import dataclass, field
from dataclasses_json import dataclass_json, LetterCase, Undefined, config

from api.models.base import BaseDataClass


@dataclass_json
@dataclass
class DefaultResourceRequirements(BaseDataClass):
    cpu_request: Optional[str] = None
    cpu_limit: Optional[str] = None
    memory_request: Optional[str] = None
    memory_limit: Optional[str] = None


@dataclass_json
@dataclass
class JSONSchema(BaseDataClass):
    type: str
    properties: dict


@dataclass_json
@dataclass
class StreamSchema(BaseDataClass):
    name: str
    json_schema: dict
    supported_sync_modes: Optional[List[str]] = None
    source_defined_cursor: Optional[bool] = None
    default_cursor_field: Optional[List[str]] = None
    source_defined_primary_key: Optional[List[List[str]]] = None
    namespace: Optional[str] = None


@dataclass_json
@dataclass
class SelectedField(BaseDataClass):
    field_path: List[str]


@dataclass_json
@dataclass
class StreamConfig(BaseDataClass):
    sync_mode: str
    cursor_field: Optional[List[str]] = None
    destination_sync_mode: Optional[str] = None
    primary_key: Optional[List[List[str]]] = None
    selected: Optional[bool] = False
    alias_name: Optional[str] = None
    suggested: Optional[bool] = None
    field_selection_enabled: Optional[bool] = False
    selected_fields: Optional[List[SelectedField]] = None


@dataclass_json
@dataclass
class StreamEntry(BaseDataClass):
    stream: StreamSchema
    config: StreamConfig


@dataclass_json
@dataclass
class SyncCatalog(BaseDataClass):
    streams: List[StreamEntry]


@dataclass_json
@dataclass
class Schedule(BaseDataClass):
    units: int
    time_unit: str


@dataclass_json
@dataclass
class CronSchedule(BaseDataClass):
    cron_expression: str
    cron_time_zone: str


@dataclass_json
@dataclass
class ScheduleData(BaseDataClass):
    basic_schedule: Optional[Schedule] = None
    cron: Optional[CronSchedule] = None


@dataclass_json
@dataclass
class ResourceRequirements(BaseDataClass):
    cpu_request: Optional[str] = None
    cpu_limit: Optional[str] = None
    memory_request: Optional[str] = None
    memory_limit: Optional[str] = None


@dataclass_json
@dataclass
class ConnectionRequest(BaseDataClass):
    name: Optional[str] = None
    namespace_definition: Optional[str] = None
    namespace_format: Optional[str] = None
    prefix: Optional[str] = None
    source_id: Optional[str] = None
    destination_id: Optional[str] = None
    operation_ids: Optional[List[str]] = None
    sync_catalog: Optional[SyncCatalog] = None
    schedule: Optional[Schedule] = None
    schedule_type: Optional[str] = None
    schedule_data: Optional[ScheduleData] = None
    status: Optional[str] = "active"
    resource_requirements: Optional[ResourceRequirements] = None
    source_catalog_id: Optional[str] = None
    geography: Optional[str] = None
    notify_schema_changes: Optional[bool] = None
    non_breaking_changes_preference: Optional[str] = None


@dataclass_json
@dataclass
class ConnectionResponse(BaseDataClass):
    connection_id: str
    name: str
