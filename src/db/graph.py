import os

from neo4j import GraphDatabase
from dotenv import load_dotenv
from timestamp_to_datetime import timestamp_to_datetime

load_dotenv()


class Graph:
    def __init__(self, driver):
        self.driver = driver

    def create_page(self, page):
        with self.driver.session() as session:
            return session.write_transaction(self._run_create_page, page)

    def csv_page(self, filename):
        with self.driver.session() as session:
            return session.write_transaction(self._run_csv_page, filename)

    def csv_all_pages(self, neo4j_dir=os.getenv('NEO4J_DIR')):
            page = "page"
            dirname = f"{neo4j_dir}/import/{page}"
            for filename in os.listdir(dirname):
                if not filename.endswith(".csv"):
                    continue
                f = os.path.join(dirname, filename)
                if os.path.isfile(f):
                    self.csv_page(f"{page}/{filename}")

    def create_pagelink(self, pagelink):
        with self.driver.session() as session:
            return session.write_transaction(self._run_create_pagelink, pagelink)

    def csv_pagelink(self, filename):
        with self.driver.session() as session:
            return session.write_transaction(self._run_csv_pagelink, filename)

    def csv_all_pagelinks(self, neo4j_dir=os.getenv('NEO4J_DIR')):
            pagelinks = "pagelinks"
            dirname = f"{neo4j_dir}/import/{pagelinks}"
            for filename in os.listdir(dirname):
                if not filename.endswith(".csv"):
                    continue
                f = os.path.join(dirname, filename)
                if os.path.isfile(f):
                    self.csv_pagelink(f"{pagelinks}/{filename}")

    @staticmethod
    def _run_create_page(self, page):
        result = self.run("CREATE "
                          "(a:Page) "
                          "SET "
                          "a.page_id = $id, "
                          "a.namespace = $namespace, "
                          "a.title = $title, "
                          "a.is_redirect = $is_redirect, "
                          "a.is_new = $is_new, "
                          "a.touched = $touched, "
                          "a.latest = $latest, "
                          "a.len = $len, "
                          "a.content_model = $content_model "
                          "RETURN "
                          "* ",
                          id=page["id"],
                          namespace=page["namespace"],
                          title=page["title"],
                          is_redirect=bool(page["is_redirect"]),
                          is_new=bool(page["is_new"]),
                          touched=timestamp_to_datetime(page["touched"]),
                          latest=page["latest"],
                          len=page["len"],
                          content_model=page["content_model"]
                          )
        try:
            return result.single()[0]
        except TypeError:
            return "No result"

    @staticmethod
    def _run_csv_page(self, filename):
        self.run("LOAD CSV FROM $filename AS line "
                 "CREATE (:Page {"
                 "    page_id: toInteger(line[0]),"
                 "    namespace: toInteger(line[1]),"
                 "    title: line[2],"
                 "    is_redirect: toBoolean(toInteger(line[4])),"
                 "    is_new: toBoolean(toInteger(line[5])),"
                 "    touched: datetime({epochmillis: apoc.date.parse(line[7], 'ms', 'yyyyMMddHHmmss')}),"
                 "    latest: toInteger(line[9]),"
                 "    len: toInteger(line[10]),"
                 "    content_model: line[11]"
                 "})",
                 filename=f"file:///{filename}"
                 )

    @staticmethod
    def _run_create_pagelink(self, pagelink):
        result = self.run("MATCH "
                          "(a:Page), "
                          "(b:Page) "
                          "WHERE a.page_id = $from_ "
                          "AND b.title = $title "
                          "CREATE (a)-[r:Pagelink]->(b)"
                          "RETURN r",
                          from_=pagelink["from"],
                          title=pagelink["title"],
                          )
        try:
            return result.single()[0]
        except TypeError:
            return "No result"

    @staticmethod
    def _run_csv_pagelink(self, filename):
        self.run("LOAD CSV FROM $filename AS line "
                 "MATCH (a:Page) "
                 "WHERE a.page_id = toInteger(line[0]) "
                 "MATCH (b:Page) "
                 "WHERE b.title = line[2] "
                 "CREATE (a)-[r:Pagelink]->(b)",
                 filename=f"file:///{filename}"
                 )


if __name__ == "__main__":
    driver = GraphDatabase.driver(uri=f"bolt://localhost:{os.getenv('NEO4J_CYPHER_PORT')}",
                                  auth=(os.getenv("NEO4J_USR"), os.getenv("NEO4J_PW")))

    with driver.session() as session:
        result = session.run("MATCH (n)"
                             "DETACH DELETE n"
                             )
        print("deleted db")

    page_creator = Graph(driver)
    print(page_creator.create_page({
        "id": 1,
        "namespace": 0,
        "title": "one",
        "is_redirect": 0,
        "is_new": 1,
        "touched": "20220101000000",
        "latest": 0,
        "len": 12345,
        "content_model": "wikitext"
    }))

    print(page_creator.create_page({
        "id": 2,
        "namespace": 0,
        "title": "two",
        "is_redirect": 0,
        "is_new": 0,
        "touched": "20220102000000",
        "latest": 1,
        "len": 54321,
        "content_model": "wikitext"
    }))

    page_creator.create_page({
        "id": 3,
        "namespace": 0,
        "title": "three",
        "is_redirect": 0,
        "is_new": 0,
        "touched": "20220103000000",
        "latest": 1,
        "len": 1111,
        "content_model": "wikitext"
    })

    page_creator.create_pagelink({
        "from": 1,
        "title": "two"
    })

    page_creator.create_pagelink({
        "from": 2,
        "title": "one"
    })

    page_creator.create_pagelink({
        "from": 3,
        "title": "one"
    })

    page_creator.create_pagelink({
        "from": 1,
        "title": "one"
    })

    page_creator.create_pagelink({
        "from": 99999,
        "title": "one"
    })

    page_creator.create_pagelink({
        "from": 1,
        "title": "does_not_exist"
    })

    driver.close()
