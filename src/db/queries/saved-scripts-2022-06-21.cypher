CALL apoc.load.directory()

MATCH (n)
RETURN count(n) as count

CREATE INDEX ON :Page(page_id)

CREATE INDEX ON :Page(title)

//delete all
MATCH (n) DETACH DELETE n

MATCH (n:Page) RETURN n LIMIT 25

//page csv
LOAD CSV FROM 'file:///test_single/page_migrations_stub.csv' AS line
CREATE (:Page {
    page_id: toInteger(line[0]),
    namespace: toInteger(line[1]),
    title: line[2],
    is_redirect: toBoolean(toInteger(line[4])),
    is_new: toBoolean(toInteger(line[5])),
    touched: datetime({epochmillis: apoc.date.parse(line[7], "ms", "yyyyMMddHHmmss")}),
    latest: toInteger(line[9]),
    len: toInteger(line[10]),
    content_model: line[11]
})

//pagelinks csv
LOAD CSV FROM 'file:///test_single/pagelink_migrations_stub.csv' AS line 
MATCH (a:Page)
WHERE a.page_id = toInteger(line[0])
AND a.namespace = toInteger(line[3])
MATCH(b:Page)
WHERE b.title = line[2]
AND b.namespace = toInteger(line[1])
CREATE (a)-[r:Pagelink]->(b)

//check if exists
UNWIND ['mygraph'] AS graph
CALL gds.graph.exists(graph)
  YIELD graphName, exists
RETURN graphName, exists

//create graph
CALL gds.graph.project('mygraph', 'Page', 'PAGELINK')
YIELD
  graphName,
  nodeProjection,
  nodeCount,
  relationshipProjection,
  relationshipCount,
  projectMillis

//dijkstra shortest path estimate
MATCH (source:Page {page_id:4}), (target:Page {page_id:14})
CALL gds.shortestPath.dijkstra.write.estimate('mygraph', {
    sourceNode: source,
    targetNode: target,
    writeRelationshipType: "PATH"
})
YIELD nodeCount, relationshipCount, bytesMin, bytesMax, requiredMemory
RETURN nodeCount, relationshipCount, bytesMin, bytesMax, requiredMemory

//dijkstra all shortest paths estimate
MATCH (source:Page {page_id:4})
CALL gds.allShortestPaths.dijkstra.write.estimate('mygraph', {
    sourceNode: source,
    writeRelationshipType: "PATH"
})
YIELD nodeCount, relationshipCount, bytesMin, bytesMax, requiredMemory
RETURN nodeCount, relationshipCount, bytesMin, bytesMax, requiredMemory

//dijkstra shortest path stream
MATCH (source:Page {page_id:4}), (target:Page {page_id:14})
CALL gds.shortestPath.dijkstra.stream('mygraph', {
    sourceNode: source,
    targetNode: target
})
YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
RETURN
    index,
    gds.util.asNode(sourceNode).page_id AS sourceNodeName,
    gds.util.asNode(targetNode).page_id AS targetNodeName,
    totalCost,
    [nodeId IN nodeIds | gds.util.asNode(nodeId).page_id] AS nodeNames,
    costs,
    nodes(path) as path
ORDER BY index

//dijkstra all shortest paths stream
MATCH (source:Page {page_id:4})
CALL gds.allShortestPaths.dijkstra.stream('mygraph', {
    sourceNode: source
})
YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
RETURN
    index,
    gds.util.asNode(sourceNode).page_id AS sourceNodeName,
    gds.util.asNode(targetNode).page_id AS targetNodeName,
    totalCost,
    [nodeId IN nodeIds | gds.util.asNode(nodeId).page_id] AS nodeNames,
    costs,
    nodes(path) as path
ORDER BY index

//drop graph
CALL gds.graph.drop('mygraph') YIELD graphName;

//list graphs
CALL gds.graph.list
