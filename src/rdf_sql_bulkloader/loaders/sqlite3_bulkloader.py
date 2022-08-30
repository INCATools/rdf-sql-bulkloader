import logging
import sqlite3
from dataclasses import dataclass
from typing import Any

from rdf_sql_bulkloader.loaders.bulkloader import BulkLoader

COLS = ["subject", "predicate", "object", "value", "datatype", "language"]

RDFTAB_INSERT = """
CREATE TABLE statements AS 
SELECT
 subject AS stanza,
 subject,
 predicate,
 value,
 datatype,
 language
FROM statement
"""

@dataclass
class SqliteBulkloader(BulkLoader):
    connection: Any = None

    def bulkload(self, path: str):
        con = sqlite3.connect(self.path)
        self.connection = con
        con.execute(self.ddl())
        tuples = []
        for t in self.statements(path):
            tuples.append(t)
        logging.info(f"Loaded {len(tuples)} loaded")
        colstr = ",".join(COLS)
        qs = ",".join(["?" for _ in COLS])
        con.executemany(f"insert into statement({colstr}) values ({qs})", tuples)
        if self.rdftab_compatibility:
            con.execute(RDFTAB_INSERT)
        con.commit()
