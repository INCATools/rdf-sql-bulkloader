from abc import ABC
from dataclasses import dataclass
from pathlib import Path
from typing import Union, Tuple, Iterator

import lightrdf

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
SUBJECT = URI
PREDICATE = URI
OBJECT_URI = URI
OBJECT_VALUE = str
OBJECT_DATATYPE = str
OBJECT_LANG = str

STATEMENT = Tuple[SUBJECT, PREDICATE, OBJECT_URI, OBJECT_VALUE, OBJECT_DATATYPE, OBJECT_LANG]

@dataclass
class BulkLoader(ABC):
    path: str

    def bulkload(self, path: str):
        raise NotImplemented

    def statements(self, path: Union[Path, str]) -> Iterator[STATEMENT]:
        doc = lightrdf.RDFDocument(str(path))
        for t in doc.search_triples(None, None, None):
            yield t

    def ddl(self) -> str:
        return DDL
