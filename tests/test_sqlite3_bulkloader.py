"""Demo version test."""

import unittest

from pyoxigraph import NamedNode

from rdf_sql_bulkloader import SqliteBulkloader
from tests import (
    ENDOMEMBRANE_SYSTEM,
    FAKE_GO_PREFIX,
    NUCLEAR_ENVELOPE,
    NUCLEUS,
    TEST_INPUT_OWL,
    TEST_LANG_INPUT_OWL,
    TEST_LANG_INPUT_TTL,
)

SUB_SVF_QUERY = """
        select svf.object from 
        statements AS subc,
        statements AS svf
        WHERE subc.subject=:subject AND 
        subc.predicate='rdfs:subClassOf' AND
        subc.object=svf.subject AND
        svf.predicate='owl:someValuesFrom'
        """

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
        "xsd:string",
        None,
        None,
    ),
    (None, "GO:0005634", "rdf:type", "owl:Class", None, None, None, None),
]

LANG_CASES = [
    (
        None,
        "http://www.w3.org/TR/rdf-syntax-grammar",
        "dce:title",
        None,
        "RDF 1.1 XML Syntax",
        "xsd:string",
        None,
        None,
    ),
    (
        None,
        "http://www.w3.org/TR/rdf-syntax-grammar",
        "dce:title",
        None,
        "RDF 1.1 XML Syntax",
        "rdf:langString",
        "en",
        None,
    ),
    (
        None,
        "http://www.w3.org/TR/rdf-syntax-grammar",
        "dce:title",
        None,
        "RDF 1.1 XML Syntax",
        "rdf:langString",
        "en-us",
        None,
    ),
    (None, "ex:buecher/baum", "dce:title", None, "Der Baum", "rdf:langString", "de", None),
    (
        None,
        "ex:buecher/baum",
        "dce:description",
        None,
        "Das Buch ist außergewöhnlich",
        "rdf:langString",
        "de",
        None,
    ),
    (None, "ex:buecher/baum", "dce:title", None, "The Tree", "rdf:langString", "en", None),
]


class TestSqlit3BulkLoader(unittest.TestCase):
    """Test sqlite3."""

    def test_bulkload(self):
        """Tests bulkload into an in-memory database."""
        loader = SqliteBulkloader(database_path=":memory:")
        loader.rdftab_compatibility = True
        loader.bulkload(TEST_INPUT_OWL)
        con = loader.connection
        cur = con.cursor()
        cur.execute("select * from statement WHERE subject=:subject", {"subject": NUCLEUS})
        stmts = list(cur.fetchall())
        # for s in stmts:
        #    print(f"S={s}")
        for case in CASES:
            self.assertIn(case, stmts)
        cur.execute("select * from statements WHERE subject=:subject", {"subject": NUCLEUS})
        stmts = list(cur.fetchall())
        # for s in stmts:
        #    print(s)
        self.assertGreater(len(stmts), 10)
        cur.execute(SUB_SVF_QUERY, {"subject": NUCLEAR_ENVELOPE})
        stmts = list(cur.fetchall())
        self.assertCountEqual([(NUCLEUS,), (ENDOMEMBRANE_SYSTEM,)], stmts)
        self.assertEqual(len(stmts), 2)

    def test_prefix_map(self):
        """Tests URI contraction."""
        loader = SqliteBulkloader(
            database_path=":memory:",
            use_shacl_namespaces=False,
            named_prefix_maps=[],
            prefix_map={FAKE_GO_PREFIX: "http://purl.obolibrary.org/obo/GO_"},
        )
        self.assertEqual(1, len(loader.prefix_map.keys()))
        example_uri = NamedNode("http://purl.obolibrary.org/obo/GO_1")
        self.assertEqual(f"{FAKE_GO_PREFIX}:1", loader.contract_uri(example_uri))
        loader.rdftab_compatibility = True
        loader.bulkload(TEST_INPUT_OWL)
        con = loader.connection
        cur = con.cursor()
        self.assertEqual(
            [(FAKE_GO_PREFIX, "http://purl.obolibrary.org/obo/GO_")],
            list(cur.execute("select * from prefix").fetchall()),
        )
        s = NUCLEUS.replace("GO", FAKE_GO_PREFIX)
        cur.execute("select * from statement WHERE subject=:subject", {"subject": s})
        stmts = list(cur.fetchall())
        # for s in stmts:
        #    print(s)
        self.assertIn(
            (
                None,
                "FAKE_GO:0005634",
                "http://purl.obolibrary.org/obo/RO_0002161",
                "http://purl.obolibrary.org/obo/NCBITaxon_2",
                None,
                None,
                None,
                None,
            ),
            stmts,
        )
        self.assertEqual(f"{FAKE_GO_PREFIX}:1", loader.contract_uri(example_uri))

    def test_lang_tags(self):
        """
        Test language tags (rdf/xml input).

        https://www.w3.org/TR/rdf-syntax-grammar/#section-Syntax-languages
        :return:
        """
        loader = SqliteBulkloader(database_path=":memory:")
        loader.bulkload(str(TEST_LANG_INPUT_OWL))
        con = loader.connection
        cur = con.cursor()
        cur.execute("select * from statement")
        stmts = list(cur.fetchall())
        for s in stmts:
            print(s)
        self.assertCountEqual(LANG_CASES, stmts)

    def test_lang_tags_ttl(self):
        """
        Test language tags (turtle input).

        https://www.w3.org/TR/rdf-syntax-grammar/#section-Syntax-languages
        :return:
        """
        loader = SqliteBulkloader(database_path=":memory:")
        loader.bulkload(TEST_LANG_INPUT_TTL, "application/turtle")
        con = loader.connection
        cur = con.cursor()
        cur.execute("select * from statement")
        stmts = list(cur.fetchall())
        self.assertCountEqual(LANG_CASES, stmts)
