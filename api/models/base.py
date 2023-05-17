import dataclasses
import inspect


@dataclasses.dataclass
class BaseDataClass:
    @classmethod
    def from_dict(cls, env):
        params = {
            k: v for k, v in env.items() if k in inspect.signature(cls).parameters
        }
        return cls(**params)
