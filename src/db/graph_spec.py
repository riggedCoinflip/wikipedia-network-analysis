import os
import unittest

from dotenv import load_dotenv
from neo4j import GraphDatabase
from graph import Graph
from stubs import page_stub, pagelink_stub, outputs_stub

load_dotenv()


class TestGraph(unittest.TestCase):
    _driver = None

    @classmethod
    def setUpClass(cls):
        cls._driver = GraphDatabase.driver(uri=f"bolt://localhost:{os.getenv('NEO4J_TEST_CYPHER_PORT')}")

    def setUp(self):
        with self._driver.session() as session:
            session.run("MATCH (n)"
                        "DETACH DELETE n"
                        )
        self.graph = Graph(self._driver)

    @classmethod
    def tearDownClass(cls):
        cls._driver.close()

    def test_page(self):
        new_page1 = self.graph.create_page(page_stub.page_dict1)
        self.assertEqual(outputs_stub.output[0][11:], str(new_page1)[11:])

        new_page2 = self.graph.create_page(page_stub.page_dict2)
        self.assertEqual(outputs_stub.output[1][11:], str(new_page2)[11:])

        new_page3 = self.graph.create_page(page_stub.page_dict3)
        self.assertEqual(outputs_stub.output[2][11:], str(new_page3)[11:])

    def test_pagelink(self):
        self.graph.create_page(page_stub.page_dict1)
        self.graph.create_page(page_stub.page_dict2)
        self.graph.create_page(page_stub.page_dict3)

        new_pagelink1 = self.graph.create_pagelink(pagelink_stub.pagelink_1_2)
        self.assertNotEqual("", str(new_pagelink1))

        new_pagelink2 = self.graph.create_pagelink(pagelink_stub.pagelink_2_1)
        self.assertNotEqual("", str(new_pagelink2))

        new_pagelink3 = self.graph.create_pagelink(pagelink_stub.pagelink_3_1)
        self.assertNotEqual("", str(new_pagelink3))

        new_pagelink4 = self.graph.create_pagelink(pagelink_stub.pagelink_1_1)
        self.assertNotEqual("", str(new_pagelink4))

        new_pagelink5 = self.graph.create_pagelink(pagelink_stub.pagelink_faultyfrom_1)
        self.assertEqual("No result", str(new_pagelink5))

        new_pagelink6 = self.graph.create_pagelink(pagelink_stub.pagelink_1_faultytitle)
        self.assertEqual("No result", str(new_pagelink6))

    def test_page_csv(self):
        self.graph.csv_page("test_single/page_migrations_stub.csv")

        #TODO assert equal

    def test_page_csv(self):
        self.graph.csv_page("test_single/page_migrations_stub.csv")
        self.graph.csv_pagelink("test_single/pagelink_migrations_stub.csv")

        #TODO assert equal

    def test_csv_all_pages(self):
        self.graph.csv_all_pages(neo4j_dir=os.getenv('NEO4J_TEST_DIR'))

        #TODO assert equal

    def test_csv_all_pages_and_pagelinks(self):
        self.graph.csv_all_pages(neo4j_dir=os.getenv('NEO4J_TEST_DIR'))
        self.graph.csv_all_pagelinks(neo4j_dir=os.getenv('NEO4J_TEST_DIR'))

        #TODO assert equal


if __name__ == '__main__':
    unittest.main()
