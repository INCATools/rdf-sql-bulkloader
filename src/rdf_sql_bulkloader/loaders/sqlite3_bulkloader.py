"""Bulk loader for SQLite3 databases."""
import itertools
import logging
import sqlite3
from dataclasses import dataclass
from typing import Any, Iterable, List, Union

from rdf_sql_bulkloader.loaders.bulkloader import DEFAULT_CHUNK, BulkLoader


logger = logging.getLogger(__name__)

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

    def create_ddl(self):
        """
        Create DDL for a given path.

        :return:
        """
        con = self.connection
        for ddl_stmt in self.ddl_statements():
            con.execute(ddl_stmt)
        con.executemany(f"insert into prefix (prefix,base) values (?,?)", self.prefix_map.items())

    def load_prefixes(self):
        con = self.connection
        raise NotImplementedError

    def bulkload(self, paths: Union[str, List[str]], mime_type=None, create_tables=True):
        """
        Bulkloads from a path.

        :param path:
        :param mime_type:
        :return:
        """
        con = sqlite3.connect(self.database_path)
        self.connection = con
        if create_tables:
            self.create_ddl()
        if not isinstance(paths, list):
            paths = [paths]
        for path in paths:
            print(path)
            logger.info(f"Loading {path} into {self.database_path} as {mime_type}...")
            colstr = ",".join(COLS)
            qs = ",".join(["?" for _ in COLS])
            for chunk_it in chunk(self.statements(path, mime_type), self.batch_size):
                tuples = []
                for t in chunk_it:
                    if self.rdftab_compatibility:
                        tuples.append(t+(t[0],))
                    else:
                        tuples.append(t)
                logging.info(f"Loaded {len(tuples)} loaded; {tuples[0]}")
                if self.rdftab_compatibility:
                    con.executemany(f"insert into statements({colstr},stanza) values ({qs},?)", tuples)
                else:
                    con.executemany(f"insert into statement({colstr}) values ({qs})", tuples)
            con.commit()
        #if self.rdftab_compatibility:
        #    con.execute(RDFTAB_INSERT)

