"""
Base class for loaders
"""
import re
from abc import ABC
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, List, Mapping, Optional, Tuple, Union

import lightrdf
from curies import Converter
from prefixmaps.io.parser import load_multi_context

re_untyped_literal = re.compile(r'^"(.*)"$')
re_typed_literal = re.compile(r'^"(.*)"\^\^<([\S^"]+)>$')
re_lang_literal = re.compile(r'^"(.*)"@([\-\w]+)$')
re_blank_node = re.compile(r'^riog(\d+)$')



DDL = """
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


def _parse_literal(o: str) -> Tuple[OBJECT_VALUE, OBJECT_DATATYPE, OBJECT_LANG]:
    """
    parses an encoded literal

    See https://github.com/ozekik/lightrdf/issues/12
    :param o:
    :return:
    """
    result = re_typed_literal.match(o)
    if result:
        o_value = result.group(1)
        o_datatype = result.group(2)
        o_lang = None
    else:
        o_datatype = None
        result = re_lang_literal.match(o)
        if result:
            o_value = result.group(1)
            o_lang = result.group(2)
        else:
            result = re_untyped_literal.match(o)
            o_lang = None
            if result:
                o_value = result.group(1)
            else:
                raise ValueError(f"Cannot parse {o}")
    return o_value, o_datatype, o_lang


def _parse_literal_as_value(o: str) -> str:
    return _parse_literal(o)[0]


@dataclass
class BulkLoader(ABC):
    """
    Base class for all bulk loaders
    """

    path: str
    named_prefix_maps: List[str] = None
    prefix_map: PREFIX_MAP = None
    converter: Converter = None
    index_statements = False
    rdftab_compatibility = True

    def __post_init__(self):
        if self.prefix_map is None:
            named_prefix_maps = self.named_prefix_maps
            if named_prefix_maps is None:
                named_prefix_maps = ["merged"]
            if len(named_prefix_maps) > 0:
                ctxt = load_multi_context(named_prefix_maps)
                self.prefix_map = ctxt.as_dict()
        self._set_converter()

    def _set_converter(self):
        if self.prefix_map:
            self.converter = Converter.from_prefix_map(self.prefix_map)

    def bulkload(self, path: str):
        raise NotImplementedError

    def _parse_node(self, o: str) -> str:
        if re_blank_node.match(o):
            return f"_:{o}"
        else:
            return self.contract_uri(o)

    def contract_uri(self, uri: Optional[URI]) -> Optional[str]:
        if uri is None:
            return None
        elif self.converter:
            curie = self.converter.compress(uri)
            if curie:
                return curie
            else:
                return uri
        else:
            return uri

    def statements(self, path: Union[Path, str]) -> Iterator[STATEMENT]:
        doc = lightrdf.RDFDocument(str(path))

        # First pass pre-processing
        # index shacl prefixes and reified statements
        # note we use lists as a proxy for mutable tuples here;
        # this may not be the cleanest way but it should hopefully be fast
        prefix_node_map = defaultdict(lambda: [None, None])
        statement_node_map = defaultdict(lambda: [None, None])
        for s, p, o in doc.search_triples(None, None, None):
            if p == "http://www.w3.org/ns/shacl#prefix":
                prefix_node_map[s][0] = _parse_literal_as_value(o)
            elif p == "http://www.w3.org/ns/shacl#namespace":
                prefix_node_map[s][1] = _parse_literal_as_value(o)
            elif self.index_statements:
                # this is optional as it may be inefficient to do at the python level
                if p == "http://www.w3.org/1999/02/22-rdf-syntax-ns#subject":
                    statement_node_map[s][0] = o
                elif p == "http://www.w3.org/1999/02/22-rdf-syntax-ns#predicate":
                    statement_node_map[s][1] = o
                elif p == "http://www.w3.org/1999/02/22-rdf-syntax-ns#object":
                    statement_node_map[s][2] = o
                elif p == "http://www.w3.org/2002/07/owl#annotatedSource":
                    statement_node_map[s][0] = o
                elif p == "http://www.w3.org/2002/07/owl#annotatedProperty":
                    statement_node_map[s][1] = o
                elif p == "http://www.w3.org/2002/07/owl#annotatedTarget":
                    statement_node_map[s][2] = o
        if prefix_node_map:
            if self.prefix_map is None:
                self.prefix_map = {}
            for [p, ns] in prefix_node_map.values():
                if p not in self.prefix_map:
                    self.prefix_map[p] = ns
            self.converter = Converter.from_prefix_map(self.prefix_map)

        # this code could be reduced if https://github.com/ozekik/lightrdf/issues/12 is implemented
        for t in doc.search_triples(None, None, None):
            s = self._parse_node(t[0])
            p = self.contract_uri(t[1])
            o = t[2]
            o_uri = None
            o_datatype = None
            o_lang = None
            if o.startswith('"'):
                o_value, o_datatype, o_lang = _parse_literal(o)
            else:
                o_value = None
                o_uri = self._parse_node(o)
            yield s, p, o_uri, o_value, o_datatype, o_lang

    def ddl(self) -> str:
        return DDL
