"""Bulk loader for SQLite3 databases."""
import itertools
import logging
import sqlite3
from dataclasses import dataclass
from typing import Any, Iterable, List

from rdf_sql_bulkloader.loaders.bulkloader import DEFAULT_CHUNK, BulkLoader

COLS = ["subject", "predicate", "object", "value", "datatype", "language"]

RDFTAB_INSERT = """
CREATE TABLE statements AS 
SELECT
 subject AS stanza,
 subject,
 predicate,
 object,
 value,
 datatype,
 language
FROM statement
"""


def chunk(iterable: Iterable, size=DEFAULT_CHUNK) -> Iterable[List]:
    """
    Get first N results of iterable

    https://stackoverflow.com/questions/8991506/iterate-an-iterator-by-chunks-of-n-in-python
    """
    it = iter(iterable)
    while True:
        chunk_it = itertools.islice(it, size)
        try:
            first_el = next(chunk_it)
        except StopIteration:
            return
        yield itertools.chain((first_el,), chunk_it)


@dataclass
class SqliteBulkloader(BulkLoader):
    """Implements BulkLoader for SQLite3 databases"""

    connection: Any = None

    def bulkload(self, path: str, mime_type=None):
        """
        Bulkloads from a path.

        :param path:
        :param mime_type:
        :return:
        """
        con = sqlite3.connect(self.database_path)
        self.connection = con
        for ddl_stmt in self.ddl_statements():
            con.execute(ddl_stmt)
        con.executemany(f"insert into prefix (prefix,base) values (?,?)", self.prefix_map.items())
        colstr = ",".join(COLS)
        qs = ",".join(["?" for _ in COLS])
        for chunk_it in chunk(self.statements(path, mime_type), self.batch_size):
            tuples = []
            for t in chunk_it:
                tuples.append(t)
            logging.info(f"Loaded {len(tuples)} loaded; {tuples[0]}")
            con.executemany(f"insert into statement({colstr}) values ({qs})", tuples)
        if self.rdftab_compatibility:
            con.execute(RDFTAB_INSERT)
        con.commit()
