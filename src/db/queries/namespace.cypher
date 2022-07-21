// 01 Reset namespace
MATCH (n:namespace) DETACH DELETE n

// 02 TEST namespace
CALL {
    MATCH (n:Page)
    RETURN n
    LIMIT 10000
}
WITH n.namespace AS ns, COUNT (DISTINCT n) as cnt
CREATE (new_ns:namespace {title: ns, value: cnt})
RETURN new_ns

// 03 TEST namespace relationships
CALL {
    MATCH (src)-[]-(target)
    RETURN src, target
    LIMIT 10000
}
WITH src.namespace AS src_ns, target.namespace AS target_ns, COUNT(DISTINCT src) AS cnt
MATCH (a:namespace) WHERE a.title = src_ns
MATCH (b:namespace) WHERE b.title = target_ns
CREATE (a)-[r:NAMESPACE_LINKS {title: a.title + ' -> ' + b.title, value: cnt}]->(b)
RETURN *

// 12 PROD namespace
MATCH (n:Page)
WITH n.namespace AS ns, COUNT (DISTINCT n) as cnt
CREATE (new_ns:namespace {title: ns, value: cnt})
RETURN new_ns

// 13 PROD namespace relationships
MATCH (src)-[]-(target)
WITH src.namespace AS src_ns, target.namespace AS target_ns, COUNT(DISTINCT src) AS cnt
MATCH (a:namespace) WHERE a.title = src_ns
MATCH (b:namespace) WHERE b.title = target_ns
CREATE (a)-[r:NAMESPACE_LINKS {title: a.title + ' -> ' + b.title, value: cnt}]->(b)
RETURN *

//99 show namespace
MATCH (n:namespace) RETURN n