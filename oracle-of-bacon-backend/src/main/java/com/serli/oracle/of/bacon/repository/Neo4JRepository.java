package com.serli.oracle.of.bacon.repository;


import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.driver.v1.types.Relationship;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class Neo4JRepository {
    private final Driver driver;
    private static final String KEVIN_BACON = "Bacon, Kevin (I)";
    private static final String USER_KEY = "neo4j";
    private static final String PASSWORD_KEY = "password";
    private static final String ACTORS_KEY = "Actors";
    private static final String NAME_KEY = "name";
    private static final String TITLE_KEY = "title";
    private static final String BOLT_SERVER_KEY = "password";

    public Neo4JRepository() {
        this.driver = GraphDatabase.driver(BOLT_SERVER_KEY, AuthTokens.basic(USER, PASSWORD));
    }

    public List<GraphItem> getConnectionsToKevinBacon(String actorName) {
        Session session = driver.session();
        Transaction transaction = session.beginTransaction();
        Statement statement = new Statement(
            "MATCH p=shortestPath((bacon {name:'" + KEVIN_BACON + "'})-[:PLAYED_IN*]-(other {name:'" + actorName + "'})) RETURN p");
        StatementResult result = transaction.run(statement);

        List<GraphItem> resultList = 
            result
                .list()
                .stream()
                .flatMap(record -> record.values().stream().map(Value::asPath))
                .flatMap(path -> generateItems(path).stream())
                .collect(Collectors.toList());

        session.close();

        return resultList;
    }

    public List<GraphItem> generateItems(Path path) {
        List<GraphItem> graphItems = new ArrayList<GraphItem>();
        addNodes(graphItems, path.nodes());
        addEdges(graphItems, path.relationships());
        return graphItems;
    }

    public void addNodes(List<GraphItem> graphItems, Iterable<Node> nodes) {
        Iterator<Node> nodeIterator = nodes.iterator();
        while(nodeIterator.hasNext()) {
            Node currentNode = nodeIterator.next();
            String type = currentNode.labels().iterator().next();
            String value = ACTORS_KEY.equals(type) ? NAME_KEY : TITLE_KEY;
            GraphNode node = new GraphNode(currentNode.id(), currentNode.get(value).asString(), type);
            graphItems.add(node);
        }
    }

    public void addEdges(List<GraphItem> graphItems, Iterable<Relationship> relationships) {
        Iterator<Relationship> relIterator = relationships.iterator();
        while(relIterator.hasNext()) {
            Relationship currentRel = relIterator.next();
            GraphEdge edge = new GraphEdge(currentRel.id(), currentRel.startNodeId(), currentRel.endNodeId(), currentRel.type());
            graphItems.add(edge);
        }
    }

    public static abstract class GraphItem {
        public final long id;

        private GraphItem(long id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            GraphItem graphItem = (GraphItem) o;

            return id == graphItem.id;
        }

        @Override
        public int hashCode() {
            return (int) (id ^ (id >>> 32));
        }
    }

    private static class GraphNode extends GraphItem {
        public final String type;
        public final String value;

        public GraphNode(long id, String value, String type) {
            super(id);
            this.value = value;
            this.type = type;
        }
    }

    private static class GraphEdge extends GraphItem {
        public final long source;
        public final long target;
        public final String value;

        public GraphEdge(long id, long source, long target, String value) {
            super(id);
            this.source = source;
            this.target = target;
            this.value = value;
        }
    }
}
