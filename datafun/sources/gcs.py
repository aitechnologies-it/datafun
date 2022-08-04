from __future__ import annotations

import base64
import binascii
import csv
import fnmatch
import json
import os
import re
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Union, List, Optional, Generator, Sequence
from urllib.parse import urlparse

from google.cloud import storage

from datafun.dataset import DatasetSource
from datafun.cache import Hashing, CacheOutput, Cache
from .local_file import FileReader

logger = logging.getLogger(__name__)


@dataclass
class GCPCacheOutput(CacheOutput):
    uri: str

    def jsonify(self):
        j = super().jsonify()
        j["uri"] = self.uri
        return j


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
