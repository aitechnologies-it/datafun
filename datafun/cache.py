from __future__ import annotations

import json
import os

from dataclasses import dataclass
from enum import Enum, auto
from typing import List


class Hashing(Enum):
    MD5 = auto()

    @classmethod
    def __repr__(cls) -> str:
        return ', '.join(cls.members())

    @classmethod
    def members(cls) -> List[str]:
        return list(Hashing.__members__.keys())


@dataclass
class CacheOutput:
    localpath: str
    metapath: str
    name: str
    folder: str
    checksum: str
    updated: str
    is_cache_miss: bool

    def jsonify(self):
        return {
            "name": self.name,
            "checksum": self.checksum,
            "updated": self.updated
        }


class Cache:
    @classmethod
    def query(cls, cache_dir: str, **kwargs) -> CacheOutput:
        if not isinstance(cache_dir, str):
            raise ValueError("cache_dir should be string, found {}")
        raise NotImplementedError()

    @staticmethod
    def is_cache_miss(path: str) -> bool:
        if os.path.exists(path):
            return False
        folder = os.path.dirname(path)
        os.makedirs(folder, exist_ok=True)
        return True

    @staticmethod
    def update_meta(output: CacheOutput):
        json.dump(output.jsonify(), open(output.metapath, "w"))
