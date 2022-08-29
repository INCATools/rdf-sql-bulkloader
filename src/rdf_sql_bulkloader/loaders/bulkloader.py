import re
from abc import ABC
from dataclasses import dataclass
from pathlib import Path
from typing import Union, Tuple, Iterator, Mapping

import lightrdf

re_untyped_literal = re.compile(r'^"(.*)"$')
re_typed_literal = re.compile(r'^"(.*)"^^<([\S^"]+)>$')
re_lang_literal = re.compile(r'^"(.*)"@(\w+)$')

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

@dataclass
class BulkLoader(ABC):
    path: str
    prefix_map: PREFIX_MAP = None

    def bulkload(self, path: str):
        raise NotImplemented

    def statements(self, path: Union[Path, str]) -> Iterator[STATEMENT]:
        doc = lightrdf.RDFDocument(str(path))
        # this code could be reduced if https://github.com/ozekik/lightrdf/issues/12 is implemented
        for t in doc.search_triples(None, None, None):
            o = t[2]
            o_uri = None
            o_datatype = None
            o_lang = None
            if o.startswith('"'):
                o_value = o
                result = re_typed_literal.match(o)
                if result:
                    o_value = result.group(1)
                    o_datatype = result.group(2)
                else:
                    result = re_lang_literal.match(o)
                    if result:
                        o_value = result.group(1)
                        o_lang = result.group(2)
            else:
                o_value = None
                o_uri = o
            yield t[0], t[1], o_uri, o_value, o_datatype, o_lang

    def ddl(self) -> str:
        return DDL
