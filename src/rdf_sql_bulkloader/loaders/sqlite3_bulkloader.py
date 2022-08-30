import sqlite3
from dataclasses import dataclass
from typing import Any

from rdf_sql_bulkloader.loaders.bulkloader import BulkLoader

COLS = ["subject", "predicate", "object", "value", "datatype", "language"]


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
        print(len(tuples))
        colstr = ",".join(COLS)
        qs = ",".join(["?" for _ in COLS])
        con.executemany(f"insert into statement({colstr}) values ({qs})", tuples)
        con.commit()
