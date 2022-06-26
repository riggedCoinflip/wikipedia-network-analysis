//01 Generate Test Graph Data
CALL apoc.generate.er(30,90,'TestPage', 'TEST_PAGELINK');

//02 Show Test Graph
MATCH (n:TestPage) RETURN n LIMIT 50

//03 create test graph
CALL gds.graph.project('testgraph', 'TestPage', 'TEST_PAGELINK')
YIELD
  graphName,
  nodeProjection,
  nodeCount,
  relationshipProjection,
  relationshipCount,
  projectMillis

//04 test dijkstra shortest path estimate
CALL gds.shortestPath.dijkstra.stream.estimate('testgraph', {
    sourceNode: 1,
    targetNode: 2
})
YIELD nodeCount, relationshipCount, bytesMin, bytesMax, requiredMemory
RETURN nodeCount, relationshipCount, bytesMin, bytesMax, requiredMemory

//05 test dijkstra shortest path stream
CALL gds.shortestPath.dijkstra.stream('testgraph', {
    sourceNode: 1,
    targetNode: 2
})
YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
RETURN
    index,
    gds.util.asNode(sourceNode).uuid AS sourceNodeName,
    gds.util.asNode(targetNode).uuid AS targetNodeName,
    totalCost,
    [nodeId IN nodeIds | gds.util.asNode(nodeId).uuid] AS nodeNames,
    costs,
    nodes(path) as path
ORDER BY index

//06 test dijkstra all shortest paths estimate
CALL gds.allShortestPaths.dijkstra.stream.estimate('testgraph', {
    sourceNode: 1
})
YIELD nodeCount, relationshipCount, bytesMin, bytesMax, requiredMemory
RETURN nodeCount, relationshipCount, bytesMin, bytesMax, requiredMemory

//07 test dijkstra all shortest paths stream
CALL gds.allShortestPaths.dijkstra.stream('testgraph', {
    sourceNode: 1
})
YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
RETURN
    index,
    gds.util.asNode(sourceNode).uuid AS sourceNodeName,
    totalCost,
    [nodeId IN nodeIds | gds.util.asNode(nodeId).uuid] AS nodeNames,
    costs,
    nodes(path) as path
ORDER BY index

//08 drop test graph
CALL gds.graph.drop('testgraph') YIELD graphName;

//09 delete test data
MATCH (n:TestPage) DETACH DELETE n

// 10 test delta all shortest paths stream
CALL gds.allShortestPaths.delta.stream('testgraph', {
    sourceNode: 1
})
YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
RETURN
    index,
    gds.util.asNode(sourceNode).uuid AS sourceNodeName,
    totalCost,
    [nodeId IN nodeIds | gds.util.asNode(nodeId).uuid] AS nodeNames,
    costs,
    nodes(path) as path
ORDER BY totalCost
