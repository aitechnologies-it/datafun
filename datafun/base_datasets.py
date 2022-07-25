from __future__ import annotations

import csv
import os
from pathlib import Path
import re
import fnmatch
import json
import glob
from typing import List, Union, Sequence, Optional, Callable, Generator, Iterable
from dataclasses import dataclass, field
from urllib.parse import urlparse
from datetime import datetime
from enum import Enum, auto
import base64
import binascii
import logging
logger = logging.getLogger(__name__)

import requests
from google.cloud import storage
import pydlib as dl
from elasticsearch import Elasticsearch, exceptions
import backoff

from .dataset import DatasetSource, Config, EmptyConfig


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

@dataclass
class GCPCacheOutput(CacheOutput):
    uri: str

    def jsonify(self):
        j = super().jsonify()
        j["uri"] = self.uri
        return j

class Cache:
    @classmethod
    def query(cls, cache_dir: str, **kwargs) -> CacheOutput:
        if not isinstance(cache_dir, "str"):
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

class GCPCache(Cache):
    @classmethod
    def query(cls, cache_dir: str, **kwargs) -> GCPCacheOutput:
        blob = kwargs.pop("blob")
        bucket = kwargs.pop("bucket")
        strategy = kwargs.pop("strategy")

        if strategy == Hashing.MD5:
            checksum = binascii.hexlify(base64.urlsafe_b64decode(blob.md5_hash)).decode('ascii')
            filename = os.path.basename(blob.name)
            folder = os.path.join(cache_dir, "hash", checksum)
            localpath = os.path.join(folder, filename)
            return GCPCacheOutput(
                localpath=localpath,
                metapath=os.path.join(folder, "meta.json"),
                name=filename,
                folder=folder,
                checksum=checksum,
                uri=cls.get_blob_uri(bucket, blob),
                updated=blob.updated.isoformat(),
                is_cache_miss=cls.is_cache_miss(localpath)
            )
        else:
            raise ValueError(f"Hashing strategy not implemented: {strategy}")

    @staticmethod
    def get_blob_uri(bucket, blob):
        return f"gs://{bucket.name}/{blob.name}"


class FileReader:
    def generate_paths_from_glob(self, path: Union[List[str], str], extensions=None) -> Generator[str, None, None]:
        if isinstance(path, str):
            path = [path]
        for p in path:
            matching_paths = glob.glob(p)
            for matching_p in matching_paths:
                if self.check_file_extension(matching_p, extensions):
                    yield matching_p

    def check_file_extension(self, path: str, extension: Optional[Union[Sequence, str]]) -> bool:
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

class IterableDataset(DatasetSource):
    def __init__(self, data: Iterable, **kwargs):
        super().__init__(config=EmptyConfig(), **kwargs)
        if data is None or not isinstance(data, Iterable):
            raise TypeError(f"data must be a Sequence, not {type(data)}")

        self.data = data

    def dataset_name(self) -> str:
        return "iterable"

    def schema(self) -> dict:
        return {}

    def _generate_examples(self) -> Generator:
        for el in self.data:
            yield el

    def clone(self) -> IterableDataset:
        return self.__class__(data=self.data, successor=None)

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
            } # We need only the first one
        except Exception as e:
            logger.error(f"Could not automatically infer schema. Did you change the type of an example with .map? Exception: {e}")
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


@dataclass
class GCSDatasetConfig:
    path: Union[List[str], str]
    download_path: str = f"{str(Path.home())}/.cache/datafun/"
    service_account: Optional[str] = None
    project: Optional[str] = None

class GCSDataset(DatasetSource):
    def __init__(self, config: GCSDatasetConfig, **kwargs):
        super().__init__(config=config, **kwargs)
        if (storage_client := kwargs.get("storage_client", None)) is not None:
            self.storage_client = storage_client
        else:
            if self.config.service_account is not None:
                self.storage_client = storage.Client.from_service_account_json(self.config.service_account)
            else:
                self.storage_client = storage.Client(project=self.config.project)

    def dataset_name(self):
        return "gcs"

    def _generate_examples(self, **kwargs) -> Generator[str, None, None]:
        wildcard_pattern = re.compile(r'((\[.+\])|\?|\*{1,2})')

        if isinstance(self.config.path, str):
            self.config.path = [self.config.path]

        for path in self.config.path:
            p = urlparse(path)
            blob_url = p.path[1:] # since goog is not able to correctly join paths with double / when downloading using gcs

            if p.scheme != 'gs' or p.netloc == '' or p.path == '':
                raise ValueError(f"Input path is not a valid gs url, got = {path}")

            bucket = self.storage_client.bucket(p.netloc)

            if wildcard_pattern.search(blob_url):
                prefix = blob_url[:wildcard_pattern.search(blob_url).span()[0]]  # type: ignore # FIXME: check type
                for blob in self.storage_client.list_blobs(bucket_or_name=bucket, prefix=prefix):
                    if fnmatch.fnmatch(blob.name, blob_url) and not blob.name.endswith('/'):
                        local_path = self.download_or_cache(blob, bucket)
                        yield local_path
            else:
                blob = bucket.get_blob(blob_url)
                if blob is not None:
                    local_path = self.download_or_cache(blob, bucket)
                    yield local_path

    def clone(self) -> GCSDataset:
        return self.__class__(config=self.config, storage_client=self.storage_client, successor=None)


    def download_or_cache(self, blob, bucket) -> str:
        cache_outputs: GCPCacheOutput = GCPCache.query(
            cache_dir=self.config.download_path,
            bucket=bucket,
            blob=blob,
            strategy=Hashing.MD5
        )
        if cache_outputs.is_cache_miss:
            blob.download_to_filename(cache_outputs.localpath)
            GCPCache.update_meta(cache_outputs)
        return cache_outputs.localpath

@dataclass
class GCSTextDatasetConfig(GCSDatasetConfig):
    encoding: str = "utf-8"

class GCSTextDataset(GCSDataset, FileReader):
    def __init__(self, config: GCSTextDatasetConfig, **kwargs):
        super().__init__(config=config, **kwargs)

    def dataset_name(self):
        return "gcs-text"

    def _generate_examples(self) -> Generator[str, None, None]:
        for path in super()._generate_examples():
            with open(path, "r", encoding=self.config.encoding) as f:
                for line in f.readlines():
                    yield line.strip()


@dataclass
class GCSCSVDatasetConfig(GCSDatasetConfig):
    encoding: str = "utf-8"
    allowed_extensions: Sequence[str] = ('csv',)
    delimiter: str = ","

class GCSCSVDataset(GCSDataset, FileReader):
    def __init__(self, config: GCSCSVDatasetConfig, **kwargs):
        super().__init__(config=config, **kwargs)

    def dataset_name(self):
        return "gcs-csv"

    def _generate_examples(self) -> Generator[dict, None, None]:
        for path in super()._generate_examples():
            if self.check_file_extension(path, self.config.allowed_extensions):
                with open(path, "r", encoding=self.config.encoding) as fp:
                    reader = csv.DictReader(fp, delimiter=self.config.delimiter)
                    for row in reader:
                        yield row


@dataclass
class GCSJSONDatasetConfig(GCSDatasetConfig):
    encoding: str = "utf-8"
    allowed_extensions: Sequence[str] = ('json',)

class GCSJSONDataset(GCSDataset, FileReader):
    def __init__(self, config: GCSJSONDatasetConfig, **kwargs):
        super().__init__(config=config, **kwargs)

    def dataset_name(self):
        return "gcs-json"

    def _generate_examples(self) -> Generator[str, None, None]:
        for path in super()._generate_examples():
            if self.check_file_extension(path, self.config.allowed_extensions):
                with open(path, "r", encoding=self.config.encoding) as f:
                    try:
                        yield json.load(f)
                    except json.JSONDecodeError:
                        logger.warning(f"Could not parse JSON file {path}")
                        continue


@dataclass
class GCSJSONLinesDatasetConfig(GCSDatasetConfig):
    encoding: str = "utf-8"
    allowed_extensions: Sequence[str] = ('json', 'jsonl',)

class GCSJSONLinesDataset(GCSDataset, FileReader):
    def __init__(self, config: GCSJSONLinesDatasetConfig, **kwargs):
        super().__init__(config=config, **kwargs)

    def dataset_name(self):
        return "gcs-jsonl"

    def _generate_examples(self) -> Generator[dict, None, None]:
        for path in super()._generate_examples():
            if self.check_file_extension(path, self.config.allowed_extensions):
                with open(path, "r", encoding=self.config.encoding) as f:
                    for i, line in enumerate(f.readlines()):
                        try:
                            yield json.loads(line.strip())
                        except json.JSONDecodeError:
                            logger.warning(f"Could not parse JSON line at index {i} for file {path}")
                            continue


@dataclass
class ELKDatasetConfig:
    path: Union[List[str], str]
    host: str
    port: int
    username: str
    password: str
    index: str
    start_isodate: str
    end_isodate: str
    date_field: str = "@timestamp"

def _backoff_hdlr(details):
    logger.info("Backing off {wait:0.1f} seconds after {tries} tries "
        "calling function {target} with args {args} and kwargs "
        "{kwargs}".format(**details))

class ELKDataset(DatasetSource):
    def __init__(self, config: ELKDatasetConfig, **kwargs):
        super().__init__(config=config, **kwargs)

        self.es = Elasticsearch(
            [f"https://{self.config.username}:{self.config.password}@{self.config.host}:{self.config.port}"]
        )

        if isinstance(self.config.path, str):
            self.config.path = [self.config.path]

        self.Qs = []
        for path in self.config.path:
            if not os.path.exists(path):
                raise ValueError(f"Query file {path} not found.")

            with open(path) as fp:
                self.Qs.append(self.set_time_interval(json.load(fp)))

    def dataset_name(self):
        return "elk"

    def _generate_examples(self) -> Generator[dict, None, None]:
        for Q in self.Qs:
            res = self._search(index=self.config.index, body=json.dumps(Q), scroll='20s')
            hits = dl.get(res, 'hits.hits', default=[])

            old_scroll_id = dl.get(res, '_scroll_id')
            while (scroll_sz := len(hits)) > 0:
                # iterate over the document hits for each 'scroll'
                for doc in hits:  # type: ignore
                    yield doc

                # make a request using the Scroll API
                try:
                    res = self._scroll(scroll_id=old_scroll_id, scroll='20s')
                    hits = dl.get(res, 'hits.hits')
                    # keep track of past scroll_id
                    old_scroll_id = dl.get(res, '_scroll_id')
                except exceptions.NotFoundError as e:
                    hits = []
                except exceptions.RequestError as e:
                    # _scroll_id is None, because the response is not "paginated"
                    hits = []

    def set_time_interval(self, query: dict) -> dict:
        if not isinstance(query, dict):
            raise ValueError(f"query must be a dict, not {type(query)}.")

        date_format = "%Y-%m-%dT%H:%M:%S.%fZ"
        try:
            datetime.strptime(self.config.start_isodate, date_format)
            datetime.strptime(self.config.end_isodate, date_format)
        except Exception:
            raise ValueError(f"Invalid date format, must be '{date_format}',"
                             f"but got {self.config.start_isodate} and {self.config.end_isodate}")

        xs = dl.get(query, 'query.bool.filter')
        if xs is None:
            raise ValueError(f'Field query.bool.filter has not been found.')
        if not isinstance(xs, List):
            raise TypeError(f'Field query.bool.filter must be of type List, but found of type {type(xs)}')

        for idx, obj in enumerate(xs):
            if dl.has(obj, "range"):
                obj["range"][self.config.date_field]["gte"] = self.config.start_isodate
                obj["range"][self.config.date_field]["lte"] = self.config.end_isodate
                # obj = dl.update(obj, path=date_field_gte, value=self.config.start_isodate)
                # obj = dl.update(obj, path=date_field_lte, value=self.config.end_isodate)
                if not obj:
                    raise ValueError(f'{self.config.date_field}.lte or {self.config.date_field}.lte fields can\'t be updated, e.g. check '
                                    'if they exist in the query.')
                xs[idx] = obj

        query = dl.update(query, path='query.bool.filter', value=xs)
        return query

    @backoff.on_exception(backoff.fibo,
                          Exception,
                          max_tries=5,
                          on_backoff=_backoff_hdlr)
    def _search(self, index: str, body: str, scroll: str):
        return self.es.search(
            index=index,
            body=body, # type: ignore # es.search is wrongly typed 😏
            scroll=scroll
        )

    @backoff.on_exception(backoff.fibo,
                          Exception,
                          max_tries=5,
                          on_backoff=_backoff_hdlr)
    def _scroll(self, scroll_id, scroll: str):
        return self.es.scroll(
            scroll_id=scroll_id,
            scroll=scroll
        )

@dataclass
class RESTDatasetConfig(Config):
    download_path: str = f"{str(Path.home())}/.cache/datafun/"
    headers: dict = field(default_factory=dict)
    auth_headers: dict = field(default_factory=dict)
    params: Optional[dict] = None
    method: str = 'GET'
    next_url_f: Callable[[dict], Optional[str]] = lambda x: None

class RESTDataset(DatasetSource):
    def __init__(self, config: RESTDatasetConfig, **kwargs):
        super().__init__(config=config, **kwargs)
        self.config: RESTDatasetConfig = self.config

        if isinstance(self.config.path, str):
            self.config.path = [self.config.path]

        if config.method == 'GET':
            self.request_f = requests.get
        elif config.method == 'POST':
            self.request_f = requests.post
        else:
            raise ValueError(f"method config must be 'GET' or 'POST', but found {config.method}.")

    def dataset_name(self):
        return "rest"

    def _generate_examples(self, **kwargs) -> Generator[dict, None, None]:
        for base_url in self.config.path:
            next_url = base_url
            while next_url is not None:
                headers = self.config.headers | self.config.auth_headers
                response = self.request_f(next_url, headers=headers, params=self.config.params)

                if response.status_code != 200:
                    logger.warning(f"Request to {self.config.path} failed with status code {response.status_code}. Skipping") 
                    continue
                json_obj = response.json()

                yield json_obj

                next_url = self.config.next_url_f(json_obj)

    def clone(self) -> RESTDataset:
        return self.__class__(config=self.config, successor=None)
