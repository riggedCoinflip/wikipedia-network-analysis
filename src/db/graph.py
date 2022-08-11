import os
import random
from datetime import datetime

from dotenv import load_dotenv
from neo4j import GraphDatabase
import json
from timestamp_to_datetime import timestamp_to_datetime

load_dotenv()


class Graph:
    def __init__(self, driver):
        self.driver = driver

    def single_query(self, query):
        with self.driver.session() as session:
            result = session.run(query)
            return [dict(i) for i in result]

    def create_page(self, page):
        with self.driver.session() as session:
            return session.write_transaction(self._run_create_page, page)

    def create_pagelink(self, pagelink):
        with self.driver.session() as session:
            return session.write_transaction(self._run_create_pagelink, pagelink)

    def csv_page(self, filename):
        with self.driver.session() as session:
            return session.write_transaction(self._run_csv_page, filename)

    def csv_pagelink(self, filename):
        with self.driver.session() as session:
            return session.write_transaction(self._run_csv_pagelink, filename)

    def csv_all_pages(self, neo4j_dir=os.getenv('NEO4J_DIR')):
        page = "page"
        dirname = f"{neo4j_dir}/import/{page}"
        for filename in os.listdir(dirname):
            if not filename.endswith(".csv"):
                continue
            f = os.path.join(dirname, filename)
            if os.path.isfile(f):
                self.csv_page(f"{page}/{filename}")

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

    def get_random_article(self):
        MIN_ID = 0
        MAX_ID = 55_000_000  # DB has 54.8 Million Data sets

        with self.driver.session() as session:
            while True:
                id = random.randint(MIN_ID, MAX_ID)

                result = session.run("MATCH (a:Article) "
                                     "WHERE id(a) = $id "
                                     "RETURN id(a)",
                                     id=id)

                try:
                    return result.single()[0]
                except TypeError:
                    pass

    def get_random_articles(self, value):
        articles = set()
        while len(articles) < value:
            new_article = self.get_random_article()
            if new_article not in articles:
                articles.add(new_article)
                yield new_article

    def get_random_articles_json(self, value):
        FILENAME = rf"{os.getenv('PROJECT_PATH')}\results\results.json"
        generator = self.get_random_articles(value)

        with open(FILENAME, "r") as f:
            data = json.load(f)

        t_start = datetime.now()
        t1 = t_start
        for i, elem in enumerate(generator):
            if not i + 1 % 50:
                t2 = datetime.now()
                print(f"random_articles: {i=}, diff: {t2 - t1}, total: {t2 - t_start}")
                t1 = t2
            if elem not in data:
                data[elem] = {}
                with open(FILENAME, "w") as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)

    def get_articles_from_json(self):
        FILENAME = rf"{os.getenv('PROJECT_PATH')}\results\results.json"

        with open(FILENAME, "r") as f:
            data = json.load(f)

            for key in data:
                yield key

    def dijkstra_single_source(self, node_id):
        dic = {}
        with self.driver.session() as session:
            result = session.run("CALL gds.allShortestPaths.dijkstra.stream('graph', { "
                                 "sourceNode: $node_id "
                                 "}) "
                                 "YIELD totalCost "
                                 "RETURN totalCost, COUNT(totalCost) as cnt "
                                 "ORDER BY totalCost ",
                                 node_id=node_id)

            for line in result:
                cost = int(line[0])
                count = line[1]
                dic[cost] = count

        return dic

    def dijkstra_single_source_multi_json(self):
        FILENAME = rf"{os.getenv('PROJECT_PATH')}\results\results.json"
        DIJKSTRA = "dijkstra"

        with open(FILENAME, "r") as f:
            data = json.load(f)

        generator = self.get_articles_from_json()

        t_start = datetime.now()
        t1 = t_start
        for i, elem in enumerate(generator):
            t2 = datetime.now()
            print(f"dijkstra: {i=}, diff: {t2 - t1}, total: {t2-t_start}")
            t1 = t2
            if DIJKSTRA not in data[elem]:
                data[elem][DIJKSTRA] = self.dijkstra_single_source(int(elem))
                with open(FILENAME, "w") as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)

    def node_info(self, node_id):
        with self.driver.session() as session:
            result = session.run("MATCH (n:Article) "
                                 "WHERE id(n) = $node_id "
                                 "RETURN *",
                                 node_id=node_id)
            return dict(result.single()[0])


    def node_infos_json(self):
        FILENAME = rf"{os.getenv('PROJECT_PATH')}\results\results.json"
        INFO = "info"

        with open(FILENAME, "r") as f:
            data = json.load(f)

        generator = self.get_articles_from_json()

        t_start = datetime.now()
        t1 = t_start
        for i, elem in enumerate(generator):
            if not i + 1 % 50:
                t2 = datetime.now()
                print(f"node_infos: {i=}, diff: {t2 - t1}, total: {t2-t_start}")
                t1 = t2
            if INFO not in data[elem]:
                data[elem][INFO] = self.node_info(int(elem))
                with open(FILENAME, "w") as f:
                    json.dump(data, f, ensure_ascii=False, indent=4, default=str)

    def neighbors(self, node_id):
        with self.driver.session() as session:
            result = session.run("MATCH (n:Article)-[:LINK]->(target:Article) "
                                 "WHERE id(n) = $node_id "
                                 "RETURN COUNT(target)",
                                 node_id=node_id)
            return result.single()[0]

    def neighbors_json(self):
        FILENAME = rf"{os.getenv('PROJECT_PATH')}\results\results.json"
        ADJACENT = "adjacent"

        with open(FILENAME, "r") as f:
            data = json.load(f)

        generator = self.get_articles_from_json()

        t_start = datetime.now()
        t1 = t_start
        for i, elem in enumerate(generator):
            if not i + 1 % 5:
                t2 = datetime.now()
                print(f"node_infos: {i=}, diff: {t2 - t1}, total: {t2-t_start}")
                t1 = t2
            if ADJACENT not in data[elem]:
                data[elem][ADJACENT] = self.neighbors(int(elem))
                with open(FILENAME, "w") as f:
                    json.dump(data, f, ensure_ascii=False, indent=4, default=str)


if __name__ == "__main__":
    driver = GraphDatabase.driver(uri=f"bolt://{os.getenv('URL')}:{os.getenv('NEO4J_CYPHER_PORT')}",
                                  auth=(os.getenv("NEO4J_USR"), os.getenv("NEO4J_PW")))

    graph = Graph(driver)

    # graph.import_csvs("page", 1, 100000)
    # graph.import_csvs("pagelinks", 1, 100000)

    #graph.get_random_articles_json(10_000)
    #graph.dijkstra_single_source_multi_json()
    #graph.node_infos_json()

    graph.neighbors_json()

    driver.close()
