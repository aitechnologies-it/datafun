from __future__ import annotations

from typing import Iterable, Generator

from datafun.dataset import DatasetSource, EmptyConfig


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
