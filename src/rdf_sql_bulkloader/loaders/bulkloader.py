"""
Base class for bulk loaders.
"""
from abc import ABC
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterator, List, Mapping, Optional, Tuple, Union

from curies import Converter
from prefixmaps.io.parser import load_multi_context
from pyoxigraph import BlankNode, Literal, NamedNode, parse

DEFAULT_MIME_TYPE = "application/rdf+xml"
SHACL_PREFIX = NamedNode("http://www.w3.org/ns/shacl#prefix")
SHACL_NAMESPACE = NamedNode("http://www.w3.org/ns/shacl#namespace")

STATEMENT_DDL = """
CREATE TABLE statement (
    id TEXT,
    subject TEXT,
    predicate TEXT,
    object TEXT,
    value TEXT,
    datatype TEXT,
    language TEXT,
    graph TEXT
);
"""

RDFTAB_STATEMENT_DDL = """
CREATE TABLE statements (
    stanza TEXT,
    subject TEXT,
    predicate TEXT,
    object TEXT,
    value TEXT,
    datatype TEXT,
    language TEXT
);
"""

PREFIX_DDL = """
CREATE TABLE prefix (
    prefix TEXT,
    base TEXT
);
"""

URI = str
PREFIX = str
SUBJECT = URI
PREDICATE = URI
OBJECT_URI = URI
OBJECT_VALUE = str
OBJECT_DATATYPE = str
OBJECT_LANG = str

STATEMENT = Tuple[SUBJECT, PREDICATE, OBJECT_URI, OBJECT_VALUE, OBJECT_DATATYPE, OBJECT_LANG]
PREFIX_MAP = Mapping[PREFIX, URI]

DEFAULT_CHUNK = 1000000


@dataclass
class BulkLoader(ABC):
    """
    Abstract base class for all bulk loaders.

    Currently there is only one implementation, for sqlite.
    """

    database_path: str
    named_prefix_maps: List[str] = None
    prefix_map: PREFIX_MAP = None
    converter: Converter = None
    index_statements: bool = False
    rdftab_compatibility: bool = True
    include_graph_name: bool = False
    graph_name_from_ontology: bool = False
    include_statement_id: bool = False
    use_shacl_namespaces: bool = True
    batch_size: int = field(default_factory=lambda: DEFAULT_CHUNK)
    _contract_uri_cache: Dict[Union[NamedNode, BlankNode], str] = field(default_factory=lambda: {})

    def __post_init__(self):
        if self.prefix_map is None:
            self.prefix_map = {}
        named_prefix_maps = self.named_prefix_maps
        if named_prefix_maps is None:
            named_prefix_maps = ["merged"]
        if len(named_prefix_maps) > 0:
            ctxt = load_multi_context(named_prefix_maps)
            self.prefix_map = {**ctxt.as_dict(), **self.prefix_map}
        self._set_converter()

    def _set_converter(self):
        if self.prefix_map:
            self.converter = Converter.from_prefix_map(self.prefix_map)
        else:
            raise ValueError("Must set prefix_map")

    def bulkload(self, path: str):
        raise NotImplementedError

    def _cached_parse_node(self, o: Union[NamedNode, BlankNode]) -> str:
        if o not in self._contract_uri_cache:
            self._contract_uri_cache[o] = self._parse_node(o)
        return self._contract_uri_cache[o]

    def _parse_node(self, o: Union[NamedNode, BlankNode]) -> str:
        if isinstance(o, BlankNode):
            return str(o)
        else:
            return self.contract_uri(o)

    def contract_uri(self, uri: Optional[NamedNode]) -> Optional[str]:
        if uri is None:
            return None
        elif self.converter:
            curie = self.converter.compress(uri.value)
            if curie:
                return curie
            else:
                return uri.value
        else:
            return uri.value

    def load_prefixes(self):
        raise NotImplementedError

    def statements(self, path: Union[Path, str], mime_type=None) -> Iterator[STATEMENT]:
        """Yields statement rows from an RDF file."""
        if mime_type is None:
            mime_type = DEFAULT_MIME_TYPE
        if self.use_shacl_namespaces:
            triple_it = parse(str(path), mime_type)

            # First pass: pre-processing
            # index shacl prefixes and reified statements
            # note we use lists as a proxy for mutable tuples here;
            # this may not be the cleanest way but it should hopefully be fast
            prefix_node_map = defaultdict(lambda: [None, None])
            for s, p, o in triple_it:
                if p == SHACL_PREFIX:
                    prefix_node_map[s][0] = o.value
                elif p == SHACL_NAMESPACE:
                    prefix_node_map[s][1] = o.value
            if prefix_node_map:
                if self.prefix_map is None:
                    self.prefix_map = {}
                for [p, ns] in prefix_node_map.values():
                    if p not in self.prefix_map:
                        self.prefix_map[p] = ns
                self._set_converter()

        # Second pass; yield rows
        # TODO: investigate if this is the most efficient way to do this
        for s, p, o in parse(str(path), mime_type):
            s = self._parse_node(s)
            p = self.contract_uri(p)
            if isinstance(o, Literal):
                yield s, p, None, o.value, self._cached_parse_node(o.datatype), o.language
            else:
                yield s, p, self._parse_node(o), None, None, None

    def ddl_statements(self) -> List[str]:
        """Return CREATE TABLE statements."""
        if self.rdftab_compatibility:
            return [RDFTAB_STATEMENT_DDL, PREFIX_DDL]
        else:
            return [STATEMENT_DDL, PREFIX_DDL]
