from __future__ import annotations

import csv
import json
import glob
from typing import List, Union, Sequence, Optional, Generator
from dataclasses import dataclass
import logging

from datafun.dataset import DatasetSource

logger = logging.getLogger(__name__)


class FileReader:
    def generate_paths_from_glob(self, path: Union[List[str], str], extensions=None) -> Generator[str, None, None]:
        if isinstance(path, str):
            path = [path]
        for p in path:
            matching_paths = glob.glob(p)
            for matching_p in matching_paths:
                if self.check_file_extension(matching_p, extensions):
                    yield matching_p

    @staticmethod
    def check_file_extension(path: str, extension: Optional[Union[Sequence, str]]) -> bool:
        """Returns true if the path is among the allowed extensions.
        If 'extension' is None, all extensions are allowed
        """
        if extension is not None and not isinstance(extension, Sequence) and not isinstance(extension, str):
            raise TypeError(f"extension must be a Sequence (tuple, list) or a string, not {type(extension)}")

        if extension is None:
            return True

        if isinstance(extension, str):
            extension = [extension]

        for ext in extension:
            if path.endswith(ext):
                return True

        return False


@dataclass
class TextDatasetConfig:
    path: Union[List[str], str]
    encoding: str = "utf-8"
    allowed_extensions: Optional[Sequence[str]] = None


class TextDataset(DatasetSource, FileReader):
    def __init__(self, config: TextDatasetConfig, **kwargs):
        super().__init__(config=config, **kwargs)

    def dataset_name(self) -> str:
        return "text"

    def info(self) -> dict:
        return {
            'author': 'foo@example.com',
            'description': 'to read raw text files'
        }

    def schema(self) -> dict:
        return {}

    def _generate_examples(self) -> Generator[str, None, None]:
        for path in self.generate_paths_from_glob(self.config.path, extensions=self.config.allowed_extensions):
            with open(path, encoding=self.config.encoding) as f:
                for line in f.readlines():
                    yield line.strip()


@dataclass
class CSVDatasetConfig:
    path: Union[List[str], str]
    encoding: str = "utf-8"
    allowed_extensions: Sequence[str] = ('csv',)
    delimiter: str = ","


class CSVDataset(DatasetSource, FileReader):
    def __init__(self, config: CSVDatasetConfig, **kwargs):
        """Loads csv files with header."""
        super().__init__(config=config, **kwargs)

    def dataset_name(self) -> str:
        return "csv"

    def info(self) -> dict:
        return {
            'description': 'CSV Dataset class',
        }

    def schema(self) -> dict:
        try:
            el = next(iter(self))
            return {
                'features': {colname: type(colvalue) for colname, colvalue in el.items()},
            }  # We need only the first one
        except Exception as e:
            logger.error(
                f"Could not automatically infer schema. Did you change the type of an example with .map? Exception: {e}")
            return {}

    def _generate_examples(self) -> Generator[dict, None, None]:
        for path in self.generate_paths_from_glob(self.config.path, self.config.allowed_extensions):
            with open(path, "r", encoding=self.config.encoding) as fp:
                reader = csv.DictReader(fp, delimiter=self.config.delimiter)
                for row in reader:
                    yield row


@dataclass
class JSONDatasetConfig:
    path: Union[List[str], str]
    encoding: str = "utf-8"
    allowed_extensions: Sequence[str] = ('json',)


class JSONDataset(DatasetSource, FileReader):
    def __init__(self, config: JSONDatasetConfig, **kwargs):
        super().__init__(config=config, **kwargs)

    def dataset_name(self):
        return "json"

    def _generate_examples(self) -> Generator[dict, None, None]:
        for path in self.generate_paths_from_glob(self.config.path, extensions=self.config.allowed_extensions):
            with open(path, "r", encoding=self.config.encoding) as f:
                try:
                    yield json.load(f)
                except json.JSONDecodeError:
                    logger.warning(f"Could not parse JSON file {path}")
                    continue


@dataclass
class JSONLinesDatasetConfig:
    path: Union[List[str], str]
    encoding: str = "utf-8"
    allowed_extensions: Sequence[str] = ("json", "jsonl")


class JSONLinesDataset(DatasetSource, FileReader):
    def __init__(self, config: JSONLinesDatasetConfig, **kwargs):
        super().__init__(config=config, **kwargs)

    def dataset_name(self):
        return "jsonl"

    def _generate_examples(self) -> Generator[dict, None, None]:
        for path in self.generate_paths_from_glob(self.config.path, extensions=self.config.allowed_extensions):
            with open(path, "r", encoding=self.config.encoding) as f:
                for i, line in enumerate(f.readlines()):
                    try:
                        yield json.loads(line.strip())
                    except json.JSONDecodeError:
                        logger.warning(f"Could not parse JSON line at index {i} for file {path}")
                        continue
