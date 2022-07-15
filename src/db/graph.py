import os

from dotenv import load_dotenv
from neo4j import GraphDatabase

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
        pagelinks = "PAGELINKS"
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
                          "CREATE (a)-[r:PAGELINK]->(b)"
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
                 "AND a.namespace = toInteger(line[3]) "
                 "MATCH (b:Page) "
                 "WHERE b.title = line[2] "
                 "AND b.namespace = toInteger(line[1]) "
                 "CREATE (a)-[r:PAGELINK]->(b)",
                 filename=f"file:///{filename}"
                 )

    def get_csvs(self, _dir):
        with self.driver.session() as session:
            result = session.run("CALL apoc.load.directory('*.csv', $_dir)", _dir=_dir)
            files = [x[0] for x in result.values()]

            return files

    def import_csvs(self, _dir, start, end):
        files = self.get_csvs(_dir)
        if start <= 0:
            start = 1
        with self.driver.session() as session:
            for filename in files[start - 1:end]:
                if _dir == "page":
                    session.write_transaction(self._run_csv_page, filename)
                elif _dir == "pagelinks":
                    session.write_transaction(self._run_csv_pagelink, filename)


if __name__ == "__main__":
    driver = GraphDatabase.driver(uri=f"bolt://{os.getenv('URL')}:{os.getenv('NEO4J_CYPHER_PORT')}",
                                  auth=(os.getenv("NEO4J_USR"), os.getenv("NEO4J_PW")))

    page_creator = Graph(driver)

    # page_creator.import_csvs("page", 1, 100000)
    # page_creator.import_csvs("pagelinks", 1, 100000)

    driver.close()
