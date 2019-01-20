package Neo4jTools;

import configurations.ProgramProperty;
import org.apache.commons.io.FileUtils;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

public class Neo4jDB {
    public GraphDatabaseService graphDB;
    public ProgramProperty prop = new ProgramProperty();
    public String DB_PATH;


    public Neo4jDB() {
        this.DB_PATH = prop.params.get("neo4jdb");
        System.out.println(this.DB_PATH);

    }

    public Neo4jDB(String subDBName) {
        this.DB_PATH = prop.params.get("neo4jdb") + "/" + subDBName + "/databases/graph.db";
//        System.out.println(this.DB_PATH);

    }

    private static void registerShutdownHook(final GraphDatabaseService graphDb) {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                graphDb.shutdown();
            }
        });
    }

    public void startDB() {
        GraphDatabaseBuilder builder = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(this.DB_PATH));
        graphDB = builder.newGraphDatabase();
        if (this.graphDB != null) {
            System.out.println("Connect the neo4j db (" + this.DB_PATH + ") success !!!!");
        } else {
            System.out.println("Connect the neo4j db (" + this.DB_PATH + ") Failure !!!!");
        }
    }

    public void closeDB() {
        this.graphDB.shutdown();
//        System.out.println("Close the neo4j db (" + this.DB_PATH + ") success !!!!");
    }

    public void deleleDB() {
        try {
            File f = new File(this.DB_PATH);
            if (f.exists()) {
                FileUtils.deleteDirectory(new File(this.DB_PATH));
                System.out.println("Delete the neo4j db (" + this.DB_PATH + ") success !!!!");
            }
        } catch (IOException e) {
//            e.printStackTrace();
            System.err.println("Delete the neo4j db (" + this.DB_PATH + ") fail !!!!");
        }
    }

    public long getNumberofEdges() {
        long result = 0;
        try (Transaction tx = this.graphDB.beginTx()) {
            ResourceIterable<Relationship> r = this.graphDB.getAllRelationships();
            tx.success();
            result = r.stream().count();
        }
        return result;
    }

    public long getNumberofNodes() {
        long result = 0;
        try (Transaction tx = this.graphDB.beginTx()) {
            ResourceIterable<Node> n = this.graphDB.getAllNodes();
            tx.success();
            result = n.stream().count();
        }
        return result;
    }

    public Node getRandomNode() {
        try (Transaction tx = this.graphDB.beginTx()) {
            ResourceIterable<Node> nIterable = this.graphDB.getAllNodes();
            ResourceIterator<Node> nIter = nIterable.iterator();

            if (nIter.hasNext()) {
                Node n = nIter.next();
                tx.success();
                return n;
            } else {
                tx.success();
                return null;
            }
        }
    }

    private int getRandomIntNumberInRange(int min, int max) {

        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        Random r = new Random();
        return r.nextInt((max - min) + 1) + min;
    }

    public ArrayList<Long> getNeighbors(long id) {
        ArrayList<Long> result = new ArrayList<>();
        try (Transaction tx = this.graphDB.beginTx()) {
            Node sn = graphDB.getNodeById(id);

            Iterable<Relationship> relIterable = sn.getRelationships(Direction.BOTH);
            Iterator<Relationship> relIter = relIterable.iterator();

            while (relIter.hasNext()) {
                Relationship rel = relIter.next();
                System.out.println(rel);
                Node en = null;
                if (rel.getEndNode().getId() == id) {
                    en = rel.getStartNode();
                } else {
                    en = rel.getEndNode();
                }


                result.add(en.getId());
            }
        }
        return result;
    }


    public Node getoutgoingNode(Relationship rel, Node v) {
        Node en = null;
        if (rel.getEndNode().getId() == v.getId()) {
            en = rel.getStartNode();
        } else {
            en = rel.getEndNode();
        }
        return en;
    }


    public ArrayList<Relationship> getoutgoingEdge(Node v) {
        ArrayList<Relationship> relationships = new ArrayList<>();
        try (Transaction tx = this.graphDB.beginTx()) {
            Iterable<Relationship> rel_Iterable = v.getRelationships(Direction.BOTH);
            Iterator<Relationship> rel_Iter = rel_Iterable.iterator();
            while (rel_Iter.hasNext()) {
                Relationship rel = rel_Iter.next();
                relationships.add(rel);
            }
            tx.success();
        }
        return relationships;
    }

    public void listallEdges() {
        try (Transaction tx = this.graphDB.beginTx()) {
            ResourceIterable<Relationship> rel_Iterable = graphDB.getAllRelationships();
            ResourceIterator<Relationship> rel_Iter = rel_Iterable.iterator();
            while (rel_Iter.hasNext()) {
                Relationship rel = rel_Iter.next();
                System.out.println(rel);
            }
            tx.success();
        }
    }

    public void listallNodes() {
        try (Transaction tx = this.graphDB.beginTx()) {
            ResourceIterable<Node> n_Iterable = graphDB.getAllNodes();
            ResourceIterator<Node> n_Iter = n_Iterable.iterator();
            while (n_Iter.hasNext()) {
                Node n = n_Iter.next();
                System.out.println(n);
            }
            tx.success();
        }
    }

    public long getRelationShipByStartAndEndNodeID(long nodeid, long next_node_id) {
        long rid = -1;
        try (Transaction tx = this.graphDB.beginTx()) {
            Node start_node = this.graphDB.getNodeById(nodeid);
            Iterable<Relationship> rel_Iterable = start_node.getRelationships(Direction.BOTH);
            Iterator<Relationship> rel_Iter = rel_Iterable.iterator();
            while (rel_Iter.hasNext()) {
                Relationship rel = rel_Iter.next();
                if (rel.getEndNodeId() == next_node_id || rel.getStartNodeId() == next_node_id) {
                    rid = rel.getId();
                    break;
                }
            }
            tx.success();

        }
        return rid;
    }
}
