from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Callable, Generator
import logging

import requests
from datafun.dataset import Config, DatasetSource

logger = logging.getLogger(__name__)


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
