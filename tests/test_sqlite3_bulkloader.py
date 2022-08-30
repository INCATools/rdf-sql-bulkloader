"""Demo version test."""

import unittest

from rdf_sql_bulkloader import SqliteBulkloader
from tests import TEST_INPUT_OWL, NUCLEUS, TEST_PREFIX_MAP


CASES = [
    (None, "GO:0005634", "RO:0002161", "NCBITaxon:2", None, None, None, None),
    (
        None,
        "GO:0005634",
        "IAO:0000115",
        None,
        "A membrane-bounded organelle of eukaryotic cells in which chromosomes are housed and replicated. In most cells, the nucleus contains all of the cell's chromosomes except the organellar chromosomes, and is the site of RNA synthesis and processing. In some species, or in specialized cell types, RNA metabolism or DNA replication may be absent.",
        "http://www.w3.org/2001/XMLSchema#string",
        None,
        None,
    ),
    (None, "GO:0005634", "rdf:type", "owl:Class", None, None, None, None),
]


class TestSqlit3BulkLoader(unittest.TestCase):
    """Test sqlite3."""

    def test_bulkload(self):
        loader = SqliteBulkloader(path=":memory:")
        loader.bulkload(TEST_INPUT_OWL)
        con = loader.connection
        cur = con.cursor()
        cur.execute("select * from statement WHERE subject=:subject", {"subject": NUCLEUS})
        stmts = list(cur.fetchall())
        for s in stmts:
            print(s)
        for case in CASES:
            self.assertIn(case, stmts)
