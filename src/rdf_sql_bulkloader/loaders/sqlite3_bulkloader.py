import sqlite3

from rdf_sql_bulkloader.loaders.bulkloader import BulkLoader


class SqliteBulkloader(BulkLoader):

    def bulkload(self, path: str):
        con = sqlite3.connect(self.path)
        con.execute(self.ddl())
        tuples = []
        for t in self.statements(path):
            tuples.append(t)
        print(len(tuples))
        con.executemany("insert into statement(subject, predicate, object) values (?,?,?)", tuples)