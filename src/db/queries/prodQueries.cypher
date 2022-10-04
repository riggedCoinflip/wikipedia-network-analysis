//01 calc estimate
CALL gds.graph.project.estimate('*', '*', {
  nodeCount: 16296665,
  relationshipCount: 583768961,
  nodeProperties: 'foo',
  relationshipProperties: 'bar'
})
YIELD requiredMemory, treeView, mapView, bytesMin, bytesMax, nodeCount, relationshipCount

//02 create graph
CALL gds.graph.project('natural_graph', 'Article',{
    LINK: {
      orientation: 'NATURAL'
    }
  })
YIELD
  graphName,
  nodeProjection,
  nodeCount,
  relationshipProjection,
  relationshipCount,
  projectMillis

//02.1 create graph undirected
CALL gds.graph.project('undirected_graph', 'Article',{
    LINK: {
      orientation: 'UNDIRECTED'
    }
  })
YIELD
  graphName,
  nodeProjection,
  nodeCount,
  relationshipProjection,
  relationshipCount,
  projectMillis

//02.2 create graph flipped
CALL gds.graph.project('reverse_graph', 'Article',{
    LINK: {
      orientation: 'REVERSE'
    }
  })
YIELD
  graphName,
  nodeProjection,
  nodeCount,
  relationshipProjection,
  relationshipCount,
  projectMillis

//03 dijkstra shortest path estimate
CALL gds.shortestPath.dijkstra.stream.estimate('graph', {
    sourceNode: 1,
    targetNode: 2
})
YIELD nodeCount, relationshipCount, bytesMin, bytesMax, requiredMemory
RETURN nodeCount, relationshipCount, bytesMin, bytesMax, requiredMemory

//04 dijkstra shortest path stream Germany->Lithography
CALL gds.shortestPath.dijkstra.stream('graph', {
    sourceNode: 285396,
    targetNode: 290341
})
YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
RETURN
    index,
    gds.util.asNode(sourceNode).title AS sourceNodeName,
    gds.util.asNode(targetNode).title AS targetNodeName,
    totalCost,
    [nodeId IN nodeIds | gds.util.asNode(nodeId).title] AS nodeNames,
    costs,
    nodes(path) as path
ORDER BY index

//05 dijkstra all shortest paths estimate
CALL gds.allShortestPaths.dijkstra.stream.estimate('graph', {
    sourceNode: 1
})
YIELD nodeCount, relationshipCount, bytesMin, bytesMax, requiredMemory
RETURN nodeCount, relationshipCount, bytesMin, bytesMax, requiredMemory

//06 dijkstra all shortest paths stream
CALL gds.allShortestPaths.dijkstra.stream('graph', {
    sourceNode: 285396
})
YIELD totalCost
RETURN totalCost, COUNT(totalCost) as cnt
ORDER BY totalCost

//07 dijkstra all shortest paths stream LONGEST PATHS
CALL gds.allShortestPaths.dijkstra.stream('graph', {
    sourceNode: 285396
})
YIELD index, sourceNode, targetNode, totalCost, nodeIds, path
WHERE totalCost >= 25
RETURN
    totalCost,
    [nodeId IN nodeIds | gds.util.asNode(nodeId).title] AS nodeNames,
    nodes(path) as path
ORDER BY totalCost

//08 pagerank estimate
CALL gds.pageRank.stream.estimate('graph', {
  maxIterations: 20,
  dampingFactor: 0.85
})
YIELD nodeCount, relationshipCount, bytesMin, bytesMax, requiredMemory

//09 pagerank
CALL gds.pageRank.stream('myGraph', {
  maxIterations: 20,
  dampingFactor: 0.85,
  scaler: "L1Norm"
})
YIELD nodeId, score
RETURN id(gds.util.asNode(nodeId)) AS id, score
ORDER BY score DESC, id ASC