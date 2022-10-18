"""Demo version test."""

import unittest

from pyoxigraph import parse

from rdf_sql_bulkloader import SqliteBulkloader
from tests import NUCLEUS, TEST_INPUT_OWL, TEST_LANG_INPUT_OWL, TEST_LANG_INPUT_TTL, TEST_PREFIX_MAP

DEFN = (
    "A membrane-bounded organelle of eukaryotic cells in which chromosomes "
    + "are housed and replicated. In most cells, the nucleus contains all of the cell's "
    + "chromosomes except the organellar chromosomes, and is the site of RNA synthesis "
    + "and processing. In some species, or in specialized cell types, "
    + "RNA metabolism or DNA replication may be absent."
)


CASES = [
    (None, "GO:0005634", "RO:0002161", "NCBITaxon:2", None, None, None, None),
    (
        None,
        "GO:0005634",
        "IAO:0000115",
        None,
        DEFN,
        "http://www.w3.org/2001/XMLSchema#string",
        None,
        None,
    ),
    (None, "GO:0005634", "rdf:type", "owl:Class", None, None, None, None),
]


class TestOxi(unittest.TestCase):
    """Test oxi."""

    def test_oxi(self):
        triples = parse(str(TEST_INPUT_OWL), "application/rdf+xml")
        print(len(list(triples)))
        print(len(list(triples)))
