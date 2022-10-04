import os
import random
from datetime import datetime
import csv
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
            if not i % 50:
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

    def dijkstra_single_source(self, node_id, direction):
        graph = f"{direction}_graph"

        dic = {}
        with self.driver.session() as session:
            result = session.run("CALL gds.allShortestPaths.dijkstra.stream($graph, { "
                                 "sourceNode: $node_id "
                                 "}) "
                                 "YIELD totalCost "
                                 "RETURN totalCost, COUNT(totalCost) as cnt "
                                 "ORDER BY totalCost ",
                                 graph=graph,
                                 node_id=node_id)

            for line in result:
                cost = int(line[0])
                count = line[1]
                dic[cost] = count

        return dic

    def dijkstra_single_source_multi_json(self, direction, limit, allow_redirects=True):
        FILENAME = rf"{os.getenv('PROJECT_PATH')}\results\results.json"
        DIJKSTRA = "dijkstra" if direction == "natural" else f"dijkstra_{direction}"

        with open(FILENAME, "r") as f:
            data = json.load(f)

        generator = self.get_articles_from_json()

        t_start = datetime.now()
        t1 = t_start
        count = 0
        for i, elem in enumerate(generator):
            if limit and count >= limit:
                break
            t2 = datetime.now()
            print(f"dijkstra: {i=}, diff: {t2 - t1}, total: {t2-t_start}")
            t1 = t2
            if DIJKSTRA not in data[elem]:
                if not allow_redirects and data[elem]["info"]["is_redirect"] == "true":
                    continue
                data[elem][DIJKSTRA] = self.dijkstra_single_source(int(elem), direction)
                with open(FILENAME, "w") as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)
                count += 1

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
            if not i % 50:
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
            if not i % 5:
                t2 = datetime.now()
                print(f"neighbors: {i=}, diff: {t2 - t1}, total: {t2-t_start}")
                t1 = t2
            if ADJACENT not in data[elem]:
                data[elem][ADJACENT] = self.neighbors(int(elem))
                with open(FILENAME, "w") as f:
                    json.dump(data, f, ensure_ascii=False, indent=4, default=str)

    def linked_by(self, node_id):
        with self.driver.session() as session:
            result = session.run("MATCH (n:Article)-[:LINK]->(target:Article) "
                                 "WHERE id(target) = $node_id "
                                 "RETURN COUNT(n)",
                                 node_id=node_id)
            return result.single()[0]

    def linked_by_json(self):
        FILENAME = rf"{os.getenv('PROJECT_PATH')}\results\results.json"
        LINKED_BY = "linked_by"

        with open(FILENAME, "r") as f:
            data = json.load(f)

        generator = self.get_articles_from_json()

        t_start = datetime.now()
        t1 = t_start

        print("start calulating linked by")
        for i, elem in enumerate(generator):
            if not i % 50:
                t2 = datetime.now()
                print(f"linked_by: {i=}, diff: {t2 - t1}, total: {t2-t_start}")
                t1 = t2
            if LINKED_BY not in data[elem]:
                data[elem][LINKED_BY] = self.linked_by(int(elem))
            with open(FILENAME, "w") as f:
                json.dump(data, f, ensure_ascii=False, indent=4, default=str)

    def pagerank(self, direction, iterations=20):
        FILENAME = rf"{os.getenv('PROJECT_PATH')}\results\pagerank_{direction}.csv"
        graph = f"{direction}_graph"

        print(f"starting pagerank {direction} calculation")

        t_start = datetime.now()
        with self.driver.session() as session:
            result = session.run("CALL gds.pageRank.stream($graph, { "
                                 "maxIterations: $iterations, "
                                 "dampingFactor: 0.85, "
                                 "scaler: 'L1Norm' "
                                 "}) "
                                 "YIELD nodeId, score "
                                 "RETURN "
                                 "id(gds.util.asNode(nodeId)) AS id, "
                                 "gds.util.asNode(nodeId).title AS title, "
                                 "score "
                                 "ORDER BY score DESC",
                                 graph=graph,
                                 iterations=55)

            t_1 = datetime.now()
            print(f"calculating pagerank {direction} took {t_1 - t_start}")

            with open(FILENAME, "w", newline='') as csv_file:
                writer = csv.writer(csv_file, delimiter=",")
                for i, record in enumerate(result):
                    if not i % 100_000:
                        t_2 = datetime.now()
                        print(f"{i=}, took {t_2 - t_1}, total {t_2 - t_start}. values: {record}")
                        t_1 = t_2
                    writer.writerow(record)

    def eigenvektor_centrality(self, direction, iterations=20):
        FILENAME = rf"{os.getenv('PROJECT_PATH')}\results\centrality_eigenvektor_{direction}.csv"
        graph = f"{direction}_graph"

        print(f"starting eigenvector centrality {direction} calculation")

        t_start = datetime.now()
        with self.driver.session() as session:
            result = session.run("CALL gds.eigenvector.stream($graph, { "
                                 "maxIterations: $iterations, "
                                 "scaler: 'L1Norm' "
                                 "}) "
                                 "YIELD nodeId, score "
                                 "RETURN "
                                 "id(gds.util.asNode(nodeId)) AS id, "
                                 "gds.util.asNode(nodeId).title AS title, "
                                 "score "
                                 "ORDER BY score DESC",
                                 graph=graph,
                                 iterations=iterations)

            t_1 = datetime.now()
            print(f"calculating eigenvector centrality {direction} took {t_1 - t_start}")

            with open(FILENAME, "w", newline='') as csv_file:
                writer = csv.writer(csv_file, delimiter=",")
                for i, record in enumerate(result):
                    if not i % 100_000:
                        t_2 = datetime.now()
                        print(f"{i=}, took {t_2 - t_1}, total {t_2 - t_start}. values: {record}")
                        t_1 = t_2
                    writer.writerow(record)

    def betweenness_centrality(self,  direction, sample_size=100, seed=42,):
        FILENAME = rf"{os.getenv('PROJECT_PATH')}\results\centrality_betweenness_{direction}.csv"
        graph = f"{direction}_graph"

        print(f"starting betweenness centrality {direction} calculation")

        t_start = datetime.now()
        with self.driver.session() as session:
            result = session.run("CALL gds.betweenness.stream($graph, { "
                                 "samplingSize: $sample, "
                                 "samplingSeed: $seed "
                                 "}) "
                                 "YIELD nodeId, score "
                                 "RETURN "
                                 "id(gds.util.asNode(nodeId)) AS id, "
                                 "gds.util.asNode(nodeId).title AS title, "
                                 "score "
                                 "ORDER BY score DESC ",
                                 graph=graph,
                                 sample=sample_size,
                                 seed=seed)

            t_1 = datetime.now()
            print(f"calculating betweenness centrality {direction} for {sample_size=} took {t_1 - t_start}")

            with open(FILENAME, "w", newline='') as csv_file:
                writer = csv.writer(csv_file, delimiter=",")
                for i, record in enumerate(result):
                    if not i % 100_000:
                        t_2 = datetime.now()
                        print(f"{i=}, took {t_2 - t_1}, total {t_2 - t_start}. values: {record}")
                        t_1 = t_2
                    writer.writerow(record)

    def degree_centrality(self, direction):
        FILENAME = rf"{os.getenv('PROJECT_PATH')}\results\centrality_degree_{direction}.csv"
        graph = f"{direction}_graph"

        print(f"starting degree centrality calculation {direction}")

        t_start = datetime.now()
        with self.driver.session() as session:
            result = session.run("CALL gds.degree.stream($graph)"
                                 "YIELD nodeId, score "
                                 "RETURN "
                                 "id(gds.util.asNode(nodeId)) AS id, "
                                 "gds.util.asNode(nodeId).title AS title, "
                                 "score "
                                 "ORDER BY score DESC",
                                 graph=graph)

            t_1 = datetime.now()
            print(f"calculating degree centrality {direction} took {t_1 - t_start}")

            with open(FILENAME, "w", newline='') as csv_file:
                writer = csv.writer(csv_file, delimiter=",")
                for i, record in enumerate(result):
                    if not i % 100_000:
                        t_2 = datetime.now()
                        print(f"{i=}, took {t_2 - t_1}, total {t_2 - t_start}. values: {record}")
                        t_1 = t_2
                    writer.writerow(record)

    def closeness_centrality(self, direction):
        FILENAME = rf"{os.getenv('PROJECT_PATH')}\results\centrality_closeness_{direction}.csv"
        graph = f"{direction}_graph"

        print("starting closeness centrality {direction} calculation")

        t_start = datetime.now()
        with self.driver.session() as session:
            result = session.run("CALL gds.beta.closeness.stream($graph)"
                                 "YIELD nodeId, score "
                                 "RETURN "
                                 "id(gds.util.asNode(nodeId)) AS id, "
                                 "gds.util.asNode(nodeId).title AS title, "
                                 "score "
                                 "ORDER BY score DESC",
                                 graph=graph)

            t_1 = datetime.now()
            print(f"calculating closeness centrality {direction} took {t_1 - t_start}")

            with open(FILENAME, "w", newline='') as csv_file:
                writer = csv.writer(csv_file, delimiter=",")
                for i, record in enumerate(result):

                    if not i % 100_000:
                        t_2 = datetime.now()
                        print(f"{i=}, took {t_2 - t_1}, total {t_2 - t_start}. values: {record}")
                        t_1 = t_2
                    writer.writerow(record)

            print(f"total {t_2 - t_start}")

    def local_clustering_coefficient(self):
        FILENAME = rf"{os.getenv('PROJECT_PATH')}\results\local_clustering_coefficient.csv"

        print("starting local clustering coefficient calculation")

        t_start = datetime.now()
        with self.driver.session() as session:
            result = session.run("CALL gds.localClusteringCoefficient.stream('undirected_graph')"
                                 "YIELD nodeId, localClusteringCoefficient "
                                 "RETURN "
                                 "id(gds.util.asNode(nodeId)) AS id, "
                                 "gds.util.asNode(nodeId).title AS title, "
                                 "localClusteringCoefficient "
                                 "ORDER BY localClusteringCoefficient DESC")

            t_1 = datetime.now()
            print(f"calculating local clustering coefficient took {t_1 - t_start}")

            with open(FILENAME, "w", newline='') as csv_file:
                writer = csv.writer(csv_file, delimiter=",")
                for i, record in enumerate(result):

                    if not i % 100_000:
                        t_2 = datetime.now()
                        print(f"{i=}, took {t_2 - t_1}, total {t_2 - t_start}. values: {record}")
                        t_1 = t_2
                    writer.writerow(record)

            print(f"total {t_2 - t_start}")


    def calc_multiple(self, direction):
        graph.pagerank(direction)
        graph.degree_centrality(direction)
        graph.eigenvektor_centrality(direction)
        graph.betweenness_centrality(direction, sample_size=500, seed=42)
        #graph.closeness_centrality(direction) #does APSP internally, way too long
        graph.dijkstra_single_source_multi_json(direction, limit=100)



def sssp_stats(direction):
    FILENAME = rf"{os.getenv('PROJECT_PATH')}\results\results.json"
    DIJKSTRA = f"dijkstra_{direction}"
    SSSP_STATS = f"sssp_stats_{direction}"
    OUT_NODES = "total_connected_out_nodes"
    SUM_DISTANCE = "total_distance"
    AVG_PATH_LENGTH = "avg_path_length"

    with open(FILENAME, "r") as f:
        data = json.load(f)

    for key in data:
        if DIJKSTRA in data[key]:
            nodes = 0
            total_distance = 0

            # there is always 1 node with distance 0 - the node itself.
            # Including it makes the results less readable and slightly wrong.
            if "0" in data[key][DIJKSTRA]:
                del data[key][DIJKSTRA]["0"]
            for distance, count in data[key][DIJKSTRA].items():
                nodes += count
                total_distance += int(distance) * count

            if nodes == 0:
                nodes = 1

            data[key][SSSP_STATS] = {
                OUT_NODES: nodes,
                SUM_DISTANCE: total_distance,
                AVG_PATH_LENGTH: total_distance / nodes
            }

    with open(FILENAME, "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def sssp_to_csv(direction):
    FILENAME = rf"{os.getenv('PROJECT_PATH')}\results\results.json"
    DIJKSTRA = f"dijkstra_{direction}"
    CSV_FILENAME = rf"{os.getenv('PROJECT_PATH')}\results\{DIJKSTRA}.csv"
    SSSP_STATS = f"sssp_stats_{direction}"
    INFO = "info"
    OUT_NODES = "total_connected_out_nodes"
    SUM_DISTANCE = "total_distance"
    AVG_PATH_LENGTH = "avg_path_length"

    with open(FILENAME, "r") as f:
        data = json.load(f)

    with open(CSV_FILENAME, "w", newline='') as csv_file:
        writer = csv.writer(csv_file, delimiter=",")

        header = ["id", "title", "articlelen", "is_new", "is_redirect", "linked_by", "avg", "nodes", "total",
                  *range(1, 41)]
        writer.writerow(header)

        for key in data:
            if SSSP_STATS in data[key]:
                distances = [0] * 40
                for distance, count in data[key][DIJKSTRA].items():
                    distances[int(distance)-1] = count

                row = [
                    key,
                    data[key][INFO]["title"],
                    data[key][INFO]["len"],
                    data[key][INFO]["is_new"],
                    data[key][INFO]["is_redirect"],
                    data[key]["linked_by"],
                    data[key][SSSP_STATS][AVG_PATH_LENGTH],
                    data[key][SSSP_STATS][OUT_NODES],
                    data[key][SSSP_STATS][SUM_DISTANCE],
                    *distances
                ]

                writer.writerow(row)


if __name__ == "__main__":
    driver = GraphDatabase.driver(uri=f"bolt://{os.getenv('URL')}:{os.getenv('NEO4J_CYPHER_PORT')}",
                                  auth=(os.getenv("NEO4J_USR"), os.getenv("NEO4J_PW")))

    graph = Graph(driver)

    # create db
    # graph.import_csvs("page", 1, 100000)
    # graph.import_csvs("pagelinks", 1, 100000)

    # get data
    #graph.get_random_articles_json(10_000)
    #graph.node_infos_json()
    #graph.neighbors_json()
    #graph.linked_by_json()
    #graph.calc_multiple("natural")
    #graph.calc_multiple("reverse")
    #graph.calc_multiple("undirected")
    #graph.local_clustering_coefficient()

    graph.eigenvektor_centrality("natural", 20)

    #driver.close()

    #beatify data
    #sssp_stats()
    #ssp_to_csv()




