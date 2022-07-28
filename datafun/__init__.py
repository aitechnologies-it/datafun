from typing import List, Iterable, Union

from .dataset import (
    Dataset,
    Config,
    Stream,
    StartOfStream,
    EndOfStream,
)
from .base_datasets import (
    IterableDataset,
    JSONDataset,
    JSONLinesDataset,
    TextDataset,
    TextDatasetConfig,
    CSVDataset,
    CSVDatasetConfig,
    JSONDataset,
    JSONDatasetConfig,
    JSONLinesDataset,
    JSONLinesDatasetConfig,
    GCSDataset,
    GCSDatasetConfig,
    GCSTextDataset,
    GCSTextDatasetConfig,
    GCSCSVDataset,
    GCSCSVDatasetConfig,
    GCSJSONDataset,
    GCSJSONDatasetConfig,
    GCSJSONLinesDataset,
    GCSJSONLinesDatasetConfig,
    ELKDataset,
    ELKDatasetConfig,
    RESTDataset,
    RESTDatasetConfig,
)
from .utils import UnrecognizedDatasetError

DATASETS = {
    'text': TextDataset,
    'csv': CSVDataset,
    'json': JSONDataset,
    'jsonl': JSONLinesDataset,
    'gcs': GCSDataset,
    'gcs-text': GCSTextDataset,
    'gcs-csv': GCSCSVDataset,
    'gcs-json': GCSJSONDataset,
    'gcs-jsonl': GCSJSONLinesDataset,
    'elk': ELKDataset,
    'rest': RESTDataset,
}

CONFIGS = {
    'text': TextDatasetConfig,
    'csv': CSVDatasetConfig,
    'json': JSONDatasetConfig,
    'jsonl': JSONLinesDatasetConfig,
    'gcs': GCSDatasetConfig,
    'gcs-text': GCSTextDatasetConfig,
    'gcs-csv': GCSCSVDatasetConfig,
    'gcs-json': GCSJSONDatasetConfig,
    'gcs-jsonl': GCSJSONLinesDatasetConfig,
    'elk': ELKDatasetConfig,
    'rest': RESTDatasetConfig,
}

def load(dataset_name_or_iterable: Union[str, Iterable], path: Union[List[str], str, None] = None, **kwargs) -> Dataset:
    if not isinstance(dataset_name_or_iterable, Iterable):
        # str is an iterable, so it passes this check, but we distinguish the two cases
        raise ValueError(f"dataset_name_or_iterable must be a string or an iterable")

    if isinstance(dataset_name_or_iterable, str):
        # Or use dataset specified in dataset_name_or_iterable
        if path is None:
            raise ValueError('You must specify the argument path')
        if dataset_name_or_iterable not in DATASETS:
            raise UnrecognizedDatasetError(f"Dataset {dataset_name_or_iterable} not found. Available: {list(DATASETS.keys())}")
        if dataset_name_or_iterable not in CONFIGS:
            raise UnrecognizedDatasetError(f"Config for dataset {dataset_name_or_iterable} not found. Available: {list(CONFIGS.keys())}")
        config = CONFIGS[dataset_name_or_iterable](path=path, **kwargs)
        return DATASETS[dataset_name_or_iterable](config=config)

    if isinstance(dataset_name_or_iterable, Iterable):
        # Load Dataset with given sequence data in dataset_name_or_iterable
        return IterableDataset(dataset_name_or_iterable)


def list() -> List[str]:
    return sorted(DATASETS.keys())
