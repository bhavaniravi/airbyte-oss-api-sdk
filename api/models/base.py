import dataclasses
import inspect
from dataclasses_json import Undefined, dataclass_json
import dataclasses_json


@dataclass_json(
    undefined=Undefined.EXCLUDE, letter_case=dataclasses_json.LetterCase.CAMEL
)
@dataclasses.dataclass
class BaseDataClass:
    pass
