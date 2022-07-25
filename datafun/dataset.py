from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
import random
from typing import (
    Callable, Type, Any, List, Sequence, Tuple, Generator, Union, Optional
)

from .utils import ProgressBar


@dataclass
class Meta:
    info: dict
    schema: dict
    name: Optional[str]

    @staticmethod
    def updated(m: Meta, info: Optional[dict] = None, schema: Optional[dict] = None, name: Optional[str] = None):
        return Meta(
            info=(info if info is not None else m.info),
            schema=(schema if schema is not None else m.schema),
            name=(name if name is not None else m.name)
        )


@dataclass
class Stream:
    data: Any
    meta: Any = None


@dataclass
class StartOfStream(Stream):
    data: Any = None
    meta: Any = None


@dataclass
class EndOfStream(Stream):
    data: Any = None
    meta: Any = None


@dataclass
class PullStream(Stream):
    data: Any = None
    meta: Any = None


class StreamedFunction(ABC):
    def __init__(self, function: Callable, name: Optional[str] = None, **kwargs):
        self.function = function
        self.name = name

    @abstractmethod
    def __call__(self, stream: Stream, **kwargs) -> Stream:
        raise NotImplementedError()


@dataclass
class StreamedFunctionBiDirectional(StreamedFunction, ABC):
    def __init__(
        self,
        function: Callable,
        predecessor: Optional[StreamedFunctionBiDirectional] = None,
        successor: Optional[StreamedFunctionBiDirectional] = None,
        name: Optional[str] = None,
        **kwargs
    ):
        super().__init__(function=function, name=name, **kwargs)
        self.predecessor = predecessor
        self.successor   = successor

    def backward(self, stream: Stream, **kwargs) -> Stream:
        raise NotImplementedError()

    def forward(self, stream: Stream, **kwargs) -> Stream:
        raise NotImplementedError()

    def root(self) -> StreamedFunctionBiDirectional:
        if self.predecessor is None:
            return self
        return self.predecessor.root()

    def summary(self) -> List[Tuple[Optional[str], str]]:
        R = self.root()
        N = R
        nodes = [(R.name, R.__class__.__name__)]
        while (N := N.successor) is not None:
            nodes.append((N.name, N.__class__.__name__))
        return nodes


class ProcessingNodeSequential(StreamedFunctionBiDirectional):
    def __init__(
        self,
        function: Callable,
        predecessor: Optional[StreamedFunctionBiDirectional] = None,
        successor: Optional[StreamedFunctionBiDirectional] = None,
        name: Optional[str] = None,
        **kwargs
    ):
        super().__init__(
            function=function,
            predecessor=predecessor,
            successor=successor,
            name=name,
            **kwargs
        )

    def forward(self, stream: Stream, **kwargs) -> Stream:
        if self.successor is not None:
            return self.successor(stream)
        return stream

@dataclass
class EmptyConfig():
    pass

@dataclass
class Config():
    path: Union[List[str], str]

class Dataset(ProcessingNodeSequential):
    def __init__(
        self,
        function: Callable,
        meta: Optional[Meta] = None,
        predecessor: Optional[Dataset] = None,
        successor: Optional[Dataset] = None,
        name: Optional[str] = None,
        **kwargs
    ):
        super().__init__(
            function=function,
            predecessor=predecessor,
            successor=successor,
            name=name,
            **kwargs
        )

        if meta is None:
            self.meta = Meta(info=self.info(), schema=self.schema(), name=self.name)
        else:
            self.meta = meta

    def __len__(self) -> int:
        raise NotImplementedError()

    def __iter__(self) -> Generator:
        raise NotImplementedError()

    def info(self) -> dict:
        raise NotImplementedError()

    def schema(self) -> dict:
        raise NotImplementedError()

    def collect(self) -> List:
        raise NotImplementedError()

    def show(self, n: int = 5) -> str:
        raise NotImplementedError()

    def take(self, n: int = 5) -> List:
        raise NotImplementedError()

    def take_while(self, f: Callable[..., bool]) -> List:
        raise NotImplementedError()

    def clone(self, **kwargs) -> Dataset:
        raise NotImplementedError()

    def add_successor(self, successor_class: Type[DatasetNode], **kwargs) -> DatasetNode:
        if not issubclass(successor_class, DatasetNode):
            raise TypeError(f"{__class__.__name__}: Successor class must be a Dataset subclass "
                    f"definition, not {type(successor_class)}.")

        new_node = successor_class(predecessor=self.clone(), successor=None, meta=self.meta, **kwargs)
        new_node.predecessor.successor = new_node # type: ignore # predecessor is me!
        return new_node

    def filter(self, f: Callable[..., bool], name: str = 'filter') -> DatasetNode:
        return self.add_successor(Filter, function=f, name=name)

    def sampling(self, p: float, seed: int = 42, name: str = 'sampling') -> DatasetNode:
        return self.add_successor(Sampling, p=p, seed=seed, name=name)

    def unique(self, by: Callable, name: str = 'unique') -> DatasetNode:
        return self.add_successor(Unique, get_field=by, name=name)

    def map(self, f: Callable, name: str = 'map') -> DatasetNode:
        return self.add_successor(Map, function=f, name=name)

    def flat_map(self, f: Callable = lambda x: x, name: str = 'flat_map') -> DatasetNode:
        return self.add_successor(FlatMap, function=f, name=name)

    def aggregate(self, init: Callable, agg: Callable, reduce: Callable = lambda x: x, name: str = 'aggregate') -> DatasetNode:
        return self.add_successor(Aggregate, init=init, agg=agg, reduce=reduce, name=name)

    def limit(self, n: int, name: str = 'limit') -> DatasetNode:
        return self.add_successor(Limit, n=n, name=name)


class DatasetSource(Dataset):
    def __init__(
        self,
        config: Any,
        successor: Optional[DatasetNode] = None,
        **kwargs
    ):
        super().__init__(
            function=self._generate_examples,
            predecessor=None,
            successor=successor,
            name=self.dataset_name(),
            **kwargs
        )

        self.config = config
        self.pbar = ProgressBar()

    def dataset_name(self) -> str:
        raise NotImplementedError()

    def info(self) -> dict:
        return {
            'author': 'AI Technologies – aitechnologies.it',
            'description': f'{self.name}'
        }

    def schema(self) -> dict:
        return {}

    def __call__(self, stream: Stream, **kwargs) -> Generator[Stream, None, None]:
        if not isinstance(stream, StartOfStream):
            raise TypeError(f"Source datasets only process StartOfStream requests.")

        # propagate StartOfStream
        _ = self.forward(stream)

        # apply ops and yield data from downstream
        for data in self.function(**kwargs):
            self.pbar.update_read()

            # y = Map(Filter(Map(...))), this represents the chain of calls in the pipeline
            y = self.forward(Stream(data=data))
            if isinstance(y, Generator): # downstream returns a generator, eg flatmap-like nodes return generators upstream
                for el in y:
                    if isinstance(el, PullStream):
                        continue
                    yield el
            elif isinstance(y, PullStream): # downstream calls for next data, eg filter-like nodes ask upstream for new data
                continue
            elif isinstance(y, EndOfStream):
                break
            else:  # Stream with data coming from terminal nodes
                yield y

        # propagate EndOfStream
        s = self.forward(EndOfStream())
        if isinstance(s, Stream) or isinstance(s, EndOfStream):
            yield s

    def __iter__(self) -> Generator:
        for stream in self(stream=StartOfStream()):
            if isinstance(stream, (EndOfStream, )):
                break
            yield stream.data

    def __len__(self) -> int:
        count = 0
        for _ in self:
            count += 1
        return count

    def collect(self) -> list:
        ds_collected = []
        self.pbar.create("Collecting examples")
        for el in self:
            ds_collected.append(el)
            self.pbar.update_pbar()
        self.pbar.close()
        return ds_collected

    def show(self, n: int = 5, sep: str = ", ") -> str:
        s = []
        for example, _ in zip(self, range(n)):  # HACK: To break after n examples
            s.append(str(example))
        return sep.join(s)

    def take(self, n: int = 5) -> list:
        examples = []
        for example, _ in zip(self, range(n)):  # HACK: To break after n examples
            examples.append(example)
        return examples

    def take_while(self, f: Callable[..., bool]) -> list:
        examples = []
        for example in self:
            if not f(example):
                break
            examples.append(example)
        return examples

    def clone(self) -> DatasetSource:
        return self.__class__(config=self.config, successor=None)

    def _generate_examples(self) -> Generator:
        raise NotImplementedError()


@dataclass
class DatasetNode(Dataset):
    def __init__(
        self,
        function: Callable,
        predecessor: Dataset,
        successor: Optional[Dataset] = None,
        meta: Optional[Meta] = None,
        name: Optional[str] = None,
        **kwargs
    ):
        super().__init__(
            function=function,
            meta=meta,
            predecessor=predecessor,
            successor=successor,
            name=name
        )

    def compute(self, stream: Stream, **kwargs) -> Stream:
        raise NotImplementedError()

    def __call__(self, stream: Stream, **kwargs) -> Stream:
        if isinstance(stream, StartOfStream):
            sos = self.on_start_of_stream(stream)
            return self.forward(sos)
        if isinstance(stream, PullStream):
            ps = self.on_pull_stream(stream)
            return ps
        if isinstance(stream, EndOfStream):
            eos = self.on_end_of_stream(stream)
            return self.forward(eos)
        return self.compute(stream)

    def on_start_of_stream(self, sos: StartOfStream, **kwargs) -> Stream:
        return sos

    def on_end_of_stream(self, eos: EndOfStream, **kwargs) -> Stream:
        return eos

    def on_pull_stream(self, ps: PullStream, **kwargs) -> Stream:
        return ps

    def root(self) -> DatasetSource:
        dataset = super().root()
        if not isinstance(dataset, DatasetSource):
            raise TypeError(f"{__class__.__name__}: First node must be of type DatasetSource, not {type(dataset)}.")
        return dataset

    @property
    def info(self) -> dict:
        return self.meta.info

    @info.setter
    def info(self, info: dict):
        self.meta.info = info

    @property
    def schema(self) -> dict:
        return self.meta.schema

    @schema.setter
    def schema(self, schema: dict):
        self.meta.schema = schema

    def show(self, n: int = 5) -> str:
        dataset: Dataset = self.root()
        return dataset.show(n)

    def __len__(self) -> int:
        dataset: Dataset = self.root()
        return len(dataset)

    def take(self, n: int = 5) -> list:
        dataset: Dataset = self.root()
        return dataset.take(n)

    def take_while(self, f: Callable[..., bool]) -> list:
        dataset: DatasetSource = self.root()
        return dataset.take_while(f)

    def collect(self) -> list:
        dataset: Dataset = self.root()
        return dataset.collect()

    def __iter__(self) -> Generator:
        dataset: Dataset = self.root()
        return dataset.__iter__()

    def clone(self) -> DatasetNode:
        replica: DatasetNode = self.replicate()
        if self.predecessor is not None:
            p = self.predecessor.clone()  # type: ignore
            replica.predecessor = p
            replica.predecessor.successor = replica
        return replica

    def replicate(self) -> DatasetNode:
        raise NotImplementedError()

@dataclass
class Filter(DatasetNode):
    def __init__(
        self,
        function: Callable[..., bool],
        predecessor: Union[DatasetSource, DatasetNode],
        successor: Optional[DatasetNode] = None,
        meta: Optional[Meta] = None,
        name: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(
            function=function,
            predecessor=predecessor,
            successor=successor,
            meta=meta,
            name=name, 
            **kwargs
        )

    def compute(self, stream: Stream, **kwargs) -> Stream:
        is_satisfied = self.function(stream.data)
        if not isinstance(is_satisfied, bool):
            raise TypeError(f"Filter: argument 'function' must return bool values, not {type(is_satisfied)}!")
        if is_satisfied:
            return self.forward(stream)
        return PullStream()

    def replicate(self) -> Filter:
        return Filter(
            function=self.function,
            predecessor=self.predecessor,  # type: ignore
            successor=self.successor,  # type: ignore
            meta=self.meta,
            name=self.name
        )


@dataclass
class Limit(DatasetNode):
    def __init__(
        self,
        n: int,
        predecessor: Union[DatasetSource, DatasetNode],
        successor: Optional[DatasetNode] = None,
        meta: Optional[Meta] = None,
        name: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(
            function=self._keep_going,
            predecessor=predecessor,
            successor=successor,
            meta=meta,
            name=name,
            **kwargs
        )
        self.n = n
        self.count = 0

    def _keep_going(self, *args, **kwargs) -> bool:
        self.count += 1
        return self.count <= self.n

    def compute(self, stream: Stream, **kwargs) -> Stream:
        keep_going = self.function(stream.data)
        if keep_going:
            return self.forward(stream)
        return EndOfStream()

    def replicate(self) -> Limit:
        return Limit(
            n=self.n,
            predecessor=self.predecessor, # type: ignore
            successor=self.successor, # type: ignore
            meta=self.meta,
            name=self.name
        )

    def on_start_of_stream(self, sos: StartOfStream, **kwargs) -> StartOfStream:
        self.count = 0
        return sos


@dataclass
class Sampling(Filter):
    def __init__(
        self,
        p: float,
        predecessor: Union[DatasetSource, DatasetNode],
        seed: int = 42,
        successor: Optional[DatasetNode] = None,
        meta: Optional[Meta] = None,
        name: Optional[str] = None,
        **kwargs,
    ):
        if not isinstance(p, float) or p < 0.0 or p > 1.0:
            raise TypeError(f"Sampling: Argument p must be a float between 0.0 and 1.0, not {p} of type {type(p)}")

        super().__init__(
            function=self._sampling,
            predecessor=predecessor,
            successor=successor,
            meta=meta,
            name=name,
            **kwargs
        )

        self.p = p
        self.seed = seed
        self._rng = None # to be set in self.on_start_of_stream()

    def _sampling(self, *args, **kwargs) -> bool:
        if self._rng is None:
            raise RuntimeError("Sampling: on_start_of_stream was never called!")
        return self._rng.random() < self.p

    def on_start_of_stream(self, sos: StartOfStream, **kwargs) -> StartOfStream:
        self._rng = random.Random(self.seed)
        return sos

    def replicate(self) -> Sampling:
        return Sampling(
            p=self.p,
            seed=self.seed,
            predecessor=self.predecessor, # type: ignore
            successor=self.successor, # type: ignore
            meta=self.meta,
            name=self.name
        )


@dataclass
class Unique(Filter):
    def __init__(
        self,
        get_field: Callable,
        predecessor: Union[DatasetSource, DatasetNode],
        successor: Optional[DatasetNode] = None,
        meta: Optional[Meta] = None,
        name: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(
            function=self._is_unique,
            predecessor=predecessor,
            successor=successor,
            meta=meta,
            name=name,
            **kwargs
        )

        self.get_field = get_field
        self.seen = {}

    def _is_unique(self, x) -> bool:
        x = self.get_field(x)
        if is_unique := (x not in self.seen):
            self.seen[x] = True
        return is_unique

    def on_start_of_stream(self, sos: StartOfStream, **kwargs) -> StartOfStream:
        self.seen = {}
        return sos

    def on_end_of_stream(self, eos: EndOfStream, **kwargs) -> EndOfStream:
        self.seen = {}
        return eos

    def replicate(self) -> Unique:
        return Unique(
            get_field=self.get_field,
            predecessor=self.predecessor, # type: ignore
            successor=self.successor, # type: ignore
            meta=self.meta,
            name=self.name
        )


@dataclass
class Map(DatasetNode):
    def __init__(
        self,
        function: Callable,
        predecessor: Union[DatasetSource, DatasetNode],
        successor: Optional[DatasetNode] = None,
        meta: Optional[Meta] = None,
        name: Optional[str] = None,
        **kwargs,
    ):
        if not isinstance(function, Callable):
            raise TypeError(f"Map: Argument function must be a callable, not {type(function)}")

        super().__init__(
            function=function, 
            predecessor=predecessor,
            successor=successor,
            meta=meta,
            name=name, 
            **kwargs
        )
    
    def compute(self, stream: Stream, **kwargs) -> Stream:
        output = self.function(stream.data)
        return self.forward(Stream(data=output))

    def replicate(self) -> Map:
        return Map(
            function=self.function,
            predecessor=self.predecessor, # type: ignore
            successor=self.successor, # type: ignore
            meta=self.meta,
            name=self.name
        )


@dataclass
class FlatMap(DatasetNode):
    def __init__(
        self,
        function: Callable,
        predecessor: Union[DatasetSource, DatasetNode],
        successor: Optional[DatasetNode] = None,
        meta: Optional[Meta] = None,
        name: Optional[str] = None,
        **kwargs,
    ):
        if not isinstance(function, Callable):
            raise TypeError(f"FlatMap: Argument function must be a callable, not {type(function)}")

        super().__init__(
            function=function, 
            predecessor=predecessor, # type: ignore
            successor=successor, # type: ignore
            meta=meta,
            name=name, 
            **kwargs
        )
    
    def compute(self, stream: Stream, **kwargs) -> Generator[Stream, None, Union[PullStream, None]]:
        output = self.function(stream.data)

        if not isinstance(output, Sequence):
            raise TypeError(f"FlatMap: Provided function output type must be a Sequence "
                    f"(even if an empty one, never None), got {type(output)}.")

        if len(output) == 0:
            return PullStream()
        y = None
        for el in output:
            y = self.forward(Stream(data=el))
            if isinstance(y, Generator):
                for sub_el in y:
                    if isinstance(sub_el, PullStream):
                        continue
                    yield sub_el
            elif isinstance(y, PullStream):
                continue
            elif isinstance(y, Stream):
                yield y

        if isinstance(y, PullStream):
            yield y

    def replicate(self) -> FlatMap:
        return FlatMap(
            function=self.function,
            predecessor=self.predecessor, # type: ignore
            successor=self.successor, # type: ignore
            meta=self.meta,
            name=self.name
        )


@dataclass
class Aggregate(DatasetNode):
    def __init__(
        self,
        init: Callable,
        agg: Callable,
        reduce: Callable,
        predecessor: Union[DatasetSource, DatasetNode],
        successor: Optional[DatasetNode] = None,
        meta: Optional[Meta] = None,
        name: Optional[str] = None,
        **kwargs,
    ):
        if not isinstance(init, Callable):
            raise TypeError(f"Aggregate: Argument init is not a function")
        if not isinstance(agg, Callable):
            raise TypeError(f"Aggregate: Argument agg is not a function")
        if not isinstance(agg, Callable):
            raise TypeError(f"Aggregate: Argument reduce is not a function")

        super().__init__(
            function=agg,
            predecessor=predecessor, # type: ignore
            successor=successor, # type: ignore
            meta=meta,
            name=name,
            **kwargs
        )

        self.init = init
        self.reduce = reduce
        self.aggregated = None

    def compute(self, stream: Stream, **kwargs) -> PullStream:
        self.aggregated = self.function(stream.data, self.aggregated)
        return PullStream()

    def on_pull_stream(self, ps: PullStream, **kwargs) -> EndOfStream:
        return EndOfStream()

    def on_start_of_stream(self, sos: StartOfStream, **kwargs) -> StartOfStream:
        self.aggregated = self.init()
        return sos

    def on_end_of_stream(self, eos: EndOfStream, **kwargs) -> Stream:
        reduced = self.reduce(self.aggregated)
        self.aggregated = None
        return Stream(data=reduced)

    def replicate(self) -> Aggregate:
        raise ValueError("You cannot add another node in the pipeline after an Aggregate.")
