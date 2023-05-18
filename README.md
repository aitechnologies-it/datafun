# 🍻 datafun [![Downloads](https://pepy.tech/badge/datafun)](https://pepy.tech/project/datafun)

The datafun allows for loading existing datasets or files with different formats and treat them as streaming pipelines. It also allows one to add operations like filter and map in a functional way executed lazily while loading data.

<!-- toc -->

- [Overview](#overview)
- [Architecture](#architecture)
- [Install](#install)
- [Usage](#usage)
- [Available datasets](#available-datasets)
- [Custom Datasets](#custom-datasets)

<!-- tocstop -->

# Overview

With datafun you can:

- Load files locally from specific file formats, eg. CSV, JSON etc.
- Load files from remote sources, eg. a CSV or a JSON from a Google Cloud Storage bucket.
- Apply streaming transformations on the fly by applying functional operations (filter, map...) to it.
- Define data transformations to be stored and later used and extended by anyone. This will contribute to an internal hub of common datasets and pipelines used many times.

# Architecture

A datafun Dataset is both a lazy stream of data and lazy sequential pipeline of operations.

- It is a stream of data: when you iterate over the dataset, one element at a time is generated and returned to the caller.
- It is a lazy sequential pipeline: every time an element needs to be returned, the pipeline of operations is executed on the current element.

**Node types.** The DatasetSource class is the first node of the pipeline and is responsible for loading the data from a local or remote storage, then passing it to the next node in the pipeline. DatasetNode objects representing filter or map operations can be added to the pipeline.

**Communication.** The pipeline communicates in a message-passing fashion. Stream objects are passed between nodes, representing the state of the stream and containing the current element. A stream may be a StartOfStream, EndOfStream, PullStream, or just Stream (when containing data). To give an example of the communication, consider the following pipeline:

```python
TextDataSource -> Map (lambda x: x.lower()) -> Filter (lambda x: "messiah" in x))
```

and the following stream of data:

```
"Dune" -> "Dune Messiah" -> "Children of Dune" -> ...
```

This is what happens when you begin iterating over the stream:

- StartOfStream triggers TextDataSource, which reads the first element `Dune`
- Stream is forwarded to Map, which transforms `Dune` into `dune`
- Stream is forwarded to Filter, the lambda inside fails the check, so a PullStream is sent backward
- PullStream arrives to Map, which sends it back to TextDataSource
- TextDataSource reads the PullStream, generates another element, `Dune Messiah`, then issues a Stream forward.
- ...
- This Stream arrives at the end, so `messiah` it is returned to the caller.
- ...

**Available transformation operations.**

| name      | Arguments                                                                         | Description                                                                                                                                                                                                                                                                                                                                              |
|-----------|-----------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| filter    | f: Callable[T, bool]                                                              | Filters elements according to **f**.                                                                                                                                                                                                                                                                                                                     |
| map       | f: Callable[T, S]                                                                 | Maps every element to another with function **f**.                                                                                                                                                                                                                                                                                                       |
| flat_map  | f: Callable[T, Iterable[S]] = lambda x: x                                         | A Flat Map takes a list and returns every element, one at a time. The given function **f**, given element of time **T**, must return an Iterable of type **S**. If not provided, **f** is the identity.                                                                                                                                                  |
| sampling  | p: float, seed: int                                                               | Samples elements according to the sampling ratio **p**: `0. < p <= 1.`                                                                                                                                                                                                                                                                                   |
| unique    | by: Callable[T, U]                                                                | Removes duplicates. **by** argument specifies a function that extracts the desired information                                                                                                                                                                                                                                                           |
| aggregate | init: Callable[[], S] <br> agg: Callable[[T, S], S] <br> reduce: Callable[[S], R] | Aggregates stream using function **f**. Hint: given initial value **init** of type **S** for <br> aggregated stream, maps **f** to stream (**value**: **T**, **aggregated**: **S**) where **value** is an <br> element of the stream, and **aggregated** is the current aggregate value. <br> **reduce** is applied to the aggregated value, if provided |
| zip       | *iterables                                                                        | Zips elements from multiple dataset, like python zip()                                                                                                                                                                                                                                                                                                   |
| join      | other: Dataset, key: Callable, key_left: Callable, key_right: Callable            | Joins two datasets based on provided key functions that specify the path in the dictionary. Either specify **key** for both, or **key_left** and **key_right**.                                                                                                                                                                                          |
| limit     | n: int                                                                            | Limits the number of elements in the streams to the first **n**                                                                                                                                                                                                                                                                                          |
| cache     |                                                                                   | Caches result of previous nodes into memory. Useful when the pipeline is executed multiple times. Be careful with memory intensive operations.                                                                                                                                                                                                           |

Datasets overloads basic python operations: ```+```, ```-```, ```*```, ```/```.

You can see examples for every operation in the [dedicated notebook](./examples/operations.ipynb).

**Available info operations.**

| name    | Arguments | Description                                           |
|---------|-----------|-------------------------------------------------------|
| info    | /         | Prints dataset description                            |
| summary | /         | Prints list of transformations in the pipeline        |
| schema  | /         | Prints schema of dataset. Useful for csv, json, jsonl |

**Available output operations.**
| name       | Arguments              | Return type | Description                                                                    |
|------------|------------------------|-------------|--------------------------------------------------------------------------------|
| show       | n: int                 | str         | Print first n elements as string                                               |
| take       | n: int                 | List[T]     | Returns the first n elements of the dataset                                    |
| take_while | f: Callable[..., bool] | List[T]     | Returns all elements satisfying f                                              |
| collect    | /                      | List[T]     | Returns a list containing all elements. Also prints progress while collecting. |

# Install

```bash
pip install datafun
```

# Usage

```python
import datafun as dfn
```

## Load a raw dataset (local|remote)

```python
ds: Dataset = dfn.load('text', path="path/to/dune_frank_herbert.txt")

for el in ds:
    # do something with el
```

## Load a dataset from iterable data structure

```python
ds = dfn.load(range(N))

ds.take(5) # take first 5 integers
>> [0, 1, 2, 3, 4]
```

## Adding ops to the pipeline, evaluated lazily when generating data
```python
ds = dfn.load('json', path="path/to/file.json")

ds2 = ds.filter(lambda x: x['KEY'] > 10)
ds2 = ds2.map(lambda x: x**2)

print(ds2.summary()) # Shows pipeline ops

for el in ds2:
    # do something with el.
    # This will execute the defined ops one element at a time
```

## Easy streaming transformation of remote dataset!
```python
# Streaming normalize on the fly
ds = (
    dfn
    .load('gcs-jsonl', path='gs://my_bucket/timeseries.jsonl')
    .map(lambda x: x['value'])
)
mean = 3.14 # let's assume to have a mean
norm_ds = ds - mean
for value in norm_ds:
    print(value)
```

You can see examples for every operation in the [dedicated notebook](./examples/operations.ipynb).

# Available datasets

| ID            | Class               | Elem. type | Description                                                                                                  |
|---------------|---------------------|------------|--------------------------------------------------------------------------------------------------------------|
| **text**      | TextDataset         | str        | Streams the local text file(s) one line at a time. Glob supported                                            |
| **csv**       | CSVDataset          | dict       | Streams the local CSV file(s) one row at a time. Glob supported                                              |
| **json**      | JSONDataset         | dict       | Streams the local JSON file(s) one at a time. Glob supported                                                 |
| **jsonl**     | JSONLinesDataset    | dict       | Streams the local JSONLines file(s) one line at a time. Glob supported                                       |
| **gcs**       | GCSDataset          | str        | Download files from Google Cloud Storage, then streams paths to be loaded locally. Glob supported            |
| **gcs-text**  | GCSTextDataset      | str        | As gcs, but also opens the text file(s) and streams one line a time. Glob supported                          |
| **gcs-csv**   | GCSCSVDataset       | dict       | As gcs, but also opens the CSV file(s) and streams one row a time. Glob supported                            |
| **gcs-json**  | GCSJSONDataset      | dict       | As gcs, but also opens the JSON file(s) and streams it one at a time. Glob supported                         |
| **gcs-jsonl** | GCSJSONLinesDataset | dict       | As gcs, but also opens the JSONLines file(s) and streams one line at a time. Glob supported                  |
| **elk**       | ELKDataset          | dict       | Connects to ELK instance, runs json query specified in `path`, then returns the resulting docs one at a time |
| **rest**      | RESTDataset         | dict       | Streams the response from a REST API.                                                                        |

## TextDataset

| Name                   | Type                   | Required | Default | Description                                                                    |
|------------------------|------------------------|----------|---------|--------------------------------------------------------------------------------|
| **path**               | Union[List[str]], str] | Yes      |         | The local path to be loaded                                                    |
| **encoding**           | str                    | No       | 'utf-8' | The encoding to be used when opening the file                                  |
| **allowed_extensions** | Sequence[str]          | No       | None    | The supported extensions (others will be discarded). None means all extensions |

**Returned element type**: ```str```

## CSVDataset

| Name                   | Type                  | Required | Default | Description                                                                    |
|------------------------|-----------------------|----------|---------|--------------------------------------------------------------------------------|
| **path**               | Union[List[str], str] | Yes      |         | The local path to be loaded                                                    |
| **encoding**           | str                   | No       | 'utf-8' | The encoding to be used when opening the file                                  |
| **allowed_extensions** | Sequence[str]         | No       | ('csv') | The supported extensions (others will be discarded). None means all extensions |
| **delimiter**          | str                   | No       | ','     | The column delimiter                                                           |

**Returned element type**: ```dict```. Each element represent a row from the CSV(s). The keys are the CSV column names.

## JSONDataset

| Name                   | Type                  | Required | Default  | Description                                                                    |
|------------------------|-----------------------|----------|----------|--------------------------------------------------------------------------------|
| **path**               | Union[List[str], str] | Yes      |          | The local path to be loaded                                                    |
| **encoding**           | str                   | No       | 'utf-8'  | The encoding to be used when opening the file                                  |
| **allowed_extensions** | Sequence[str]         | No       | ('json') | The supported extensions (others will be discarded). None means all extensions |

**Returned element type**: ```dict```. This dictionary directly maps the JSON file.

## JSONLinesDataset

| Name                   | Type                  | Required | Default           | Description                                                                    |
|------------------------|-----------------------|----------|-------------------|--------------------------------------------------------------------------------|
| **path**               | Union[List[str], str] | Yes      |                   | The local path to be loaded                                                    |
| **encoding**           | str                   | No       | 'utf-8'           | The encoding to be used when opening the file                                  |
| **allowed_extensions** | Sequence[str]         | No       | ('json', 'jsonl') | The supported extensions (others will be discarded). None means all extensions |

**Returned element type**: ```dict```. This dictionary maps a single JSON line from the file.

## GCSDataset

| Name                | Type                  | Required | Default                         | Description                                                                   |
|---------------------|-----------------------|----------|---------------------------------|-------------------------------------------------------------------------------|
| **path**            | Union[List[str], str] | Yes      |                                 | The remote path to be loaded                                                  |
| **download_path**   | str                   | No       | '$HOME/.cache/data-engineering' | The path where the file(s) will be downloaded                                 |
| **service_account** | str                   | No       | None                            | The service account path to use. If missing will use the default user account |
| **project**         | str                   | No       | None                            | The project id to use. If missing will use the user configured project it     |

**Returned element type**: ```str```. Each element represent a local path where to find the file downloaded from GCS.

## GCSTextDataset

| Name                | Type                  | Required | Default                         | Description                                                                   |
|---------------------|-----------------------|----------|---------------------------------|-------------------------------------------------------------------------------|
| **path**            | Union[List[str], str] | Yes      |                                 | The remote path to be loaded                                                  |
| **download_path**   | str                   | No       | '$HOME/.cache/data-engineering' | The path where the file(s) will be downloaded                                 |
| **service_account** | str                   | No       | None                            | The service account path to use. If missing will use the default user account |
| **project**         | str                   | No       | None                            | The project id to use. If missing will use the user configured project it     |
| **encoding**        | str                   | No       | 'utf-8'                         | The encoding to be used when opening the file                                 |

**Returned element type**: ```str```. Each element represent a line from the remote file.

## GCSCSVDataset

| Name                   | Type                  | Required | Default                         | Description                                                                    |
|------------------------|-----------------------|----------|---------------------------------|--------------------------------------------------------------------------------|
| **path**               | Union[List[str], str] | Yes      |                                 | The remote path to be loaded                                                   |
| **download_path**      | str                   | No       | '$HOME/.cache/data-engineering' | The path where the file(s) will be downloaded                                  |
| **service_account**    | str                   | No       | None                            | The service account path to use. If missing will use the default user account  |
| **project**            | str                   | No       | None                            | The project id to use. If missing will use the user configured project it      |
| **encoding**           | str                   | No       | 'utf-8'                         | The encoding to be used when opening the file                                  |
| **allowed_extensions** | Sequence[str]         | No       | ('csv')                         | The supported extensions (others will be discarded). None means all extensions |
| **delimiter**          | str                   | No       | ','                             | The column delimiter                                                           |

**Returned element type**: ```dict```. Each element represent a row from the CSV(s). The keys are the CSV column names.

## GCSJSONDataset

| Name                   | Type                  | Required | Default                         | Description                                                                    |
|------------------------|-----------------------|----------|---------------------------------|--------------------------------------------------------------------------------|
| **path**               | Union[List[str], str] | Yes      |                                 | The remote path to be loaded                                                   |
| **download_path**      | str                   | No       | '$HOME/.cache/data-engineering' | The path where the file(s) will be downloaded                                  |
| **service_account**    | str                   | No       | None                            | The service account path to use. If missing will use the default user account  |
| **project**            | str                   | No       | None                            | The project id to use. If missing will use the user configured project it      |
| **encoding**           | str                   | No       | 'utf-8'                         | The encoding to be used when opening the file                                  |
| **allowed_extensions** | Sequence[str]         | No       | ('json')                        | The supported extensions (others will be discarded). None means all extensions |

**Returned element type**: ```dict```. Each element is a dict directly mapping a JSON file.

## GCSJSONLinesDataset

| Name                   | Type                  | Required | Default                         | Description                                                                    |
|------------------------|-----------------------|----------|---------------------------------|--------------------------------------------------------------------------------|
| **path**               | Union[List[str], str] | Yes      |                                 | The remote path to be loaded                                                   |
| **download_path**      | str                   | No       | '$HOME/.cache/data-engineering' | The path where the file(s) will be downloaded                                  |
| **service_account**    | str                   | No       | None                            | The service account path to use. If missing will use the default user account  |
| **project**            | str                   | No       | None                            | The project id to use. If missing will use the user configured project it      |
| **encoding**           | str                   | No       | 'utf-8'                         | The encoding to be used when opening the file                                  |
| **allowed_extensions** | Sequence[str]         | No       | ('json', 'jsonl')               | The supported extensions (others will be discarded). None means all extensions |

**Returned element type**: ```dict```. Each element is a dict mapping a JSON line from the file(s).

## ELKDataset

| Name              | Type                  | Required | Default    | Description                                                      |
|-------------------|-----------------------|----------|------------|------------------------------------------------------------------|
| **path**          | Union[List[str], str] | Yes      |            | The local path to a query written in JSON                        |
| **host**          | str                   | Yes      |            | Elastic host URL                                                 |
| **port**          | str                   | Yes      |            | Elastic host port                                                |
| **username**      | str                   | Yes      |            | Elastic authentication username                                  |
| **password**      | str                   | Yes      |            | Elastic authentication password                                  |
| **index**         | str                   | Yes      |            | Elastic index to query                                           |
| **start_isodate** | str (ISO datetime)    | Yes      |            | Elastic start date range with format: "2021-09-15T10:00:00.000Z" |
| **end_isodate**   | str (ISO datetime)    | Yes      |            | Elastic end date range with format: "2021-09-15T10:00:00.000Z"   |
| **date_field**    | str                   | No       | @timestamp | Elastic date field. Can be nested into list, eg. "messages.date" |
| **date_field_separator** | str            | No       | .          | Separator for date_field used to split the path. Use different ones to NOT split and consider date_field as single field |

**Returned element type**: ```dict```. Each element is a document matching the given query.

## RESTDataset

| Name              | Type                            | Required | Default                         | Description                                                                                                                              |
|-------------------|---------------------------------|----------|---------------------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| **download_path** | str                             | No       | '$HOME/.cache/data-engineering' | Where to download data                                                                                                                   |
| **headers**       | dict                            | No       | {}                              | Call Headers                                                                                                                             |
| **auth_headers**  | dict                            | No       | {}                              | Authentication headers                                                                                                                   |
| **params**        | Optional[dict]                  | No       | None                            | URL key value params                                                                                                                     |
| **method**        | str                             | No       | 'GET'                           | Call method, 'GET', 'POST'...                                                                                                            |
| **next_url_f**    | Callable[[dict], Optional[str]] | No       | lambda x: None                  | Function to extract the next url to call from the json response for paginated APIs. Should return None to mark the end or the URL string |

**Returned element type**: ```dict``` representing the json response.

# Custom datasets

## Method 1. Functional style

```python
def my_load(path, **kwargs):
    ds = (dfn
        .load('json', path=path, **kwargs)
        .filter(lambda x: x['KEY'] > 10)
        .map(lambda x: x**2)
    )
    return ds
```

## Method 2. Extend Dataset

**Write classes**

```python
@dataclass
class ELKDatasetConfig(dfn.Config):
    # Additional arguments to base class
    host: str
    port: int

class ELKDataset(dfn.DatasetSource):
    def __init__(self, config, **kwargs):
        super().__init__(config=config, **kwargs)
        self.elk_client = elklib(self.config.host, self.config.port)

    def dataset_name() -> str:
        return 'elk-dataset'

    def info(self) -> dict:
        return {
            'description': 'My ELK dataset class.',
            'author': 'foo@company.org',
            'date': '2021-09-01',
        }

    def schema(self) -> dict:
        return {
            'column_1': 'int',
            ...
        }
        # this method may also load a row and infer it, like CSVDataset does

    def _generate_examples(self) -> Generator:
        for doc in self.elk_client.scroll_function():
            # Apply some prep to the document
            yield doc
```

**Add new dataset to list of supported datasets**

- Add a line to ```DATASETS``` in ```datafun/__init__.py```:

```
DATASETS = {
    ...
    "elk-dataset": ELKDataset,
}
```

**Use it!**

```
ds = dfn.load('elk-dataset', config={
    'host': 'localhost',
    'port': '8888',
})

```
