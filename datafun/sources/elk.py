from __future__ import annotations

import json
import os

from dataclasses import dataclass
from datetime import datetime
from typing import Union, List, Generator

import backoff
import pydlib as dl
from elasticsearch import Elasticsearch, exceptions

from datafun.utils import _backoff_hdlr
from datafun.dataset import DatasetSource


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
