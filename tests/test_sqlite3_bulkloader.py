"""Demo version test."""

import unittest

from rdf_sql_bulkloader import SqliteBulkloader
from tests import TEST_INPUT_OWL


class TestSqlit3BulkLoader(unittest.TestCase):
    """Test sqlite3."""

    def test_bulkload(self):
        loader = SqliteBulkloader(":memory:")
        loader.bulkload(TEST_INPUT_OWL)
        con = loader.connection
        cur = con.cursor()
        cur.execute("select * from statement LIMIT 5")
        for s in cur.fetchall():
            print(s)

