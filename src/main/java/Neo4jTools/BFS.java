package Neo4jTools;

import org.neo4j.graphdb.*;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;

public class BFS {


    private final Neo4jDB neo4j;
    private final GraphDatabaseService graphdb;
    public long numberofnodes = 0;

    public BFS(String sub_db_name) {
        neo4j = new Neo4jDB(sub_db_name);
//        System.out.println(neo4j.DB_PATH);
        neo4j.startDB(false);
        graphdb = neo4j.graphDB;
        if (graphdb != null) {
            this.numberofnodes = neo4j.getNumberofNodes();
            System.out.println("Connected to Graph DB success (" + numberofnodes + " nodes): " + neo4j.graphDB);
        }
    }

    public static void main(String args[]) {
        int graphsize = 10000;
        double samenode_t = 2.844;
        int currentLevel = 7;
//        for (; currentLevel <= 8; currentLevel++) {
//            String sub_db_name = graphsize + "_" + samenode_t + "_Level" + currentLevel;
//            BFS bfs = new BFS(sub_db_name);
//            bfs.traversal();
//            bfs.closeDB();
//        }

        String sub_db_name = graphsize + "_" + samenode_t + "_Level" + currentLevel;
        BFS bfs = new BFS(sub_db_name);
        bfs.traversal();
        bfs.closeDB();
    }

    private void closeDB() {
        neo4j.closeDB();
    }

    private boolean traversal() {
        if (numberofnodes == 0) {
            return false;
        }
//        boolean[] visited = new boolean[Math.toIntExact(numberofnodes)];
        HashSet<Long> visited_map = new HashSet<>();

        try (Transaction tx = this.graphdb.beginTx()) {

            Node first_node = this.graphdb.getAllNodes().iterator().next();
//            System.out.println(first_node);
            Queue<Node> q = new LinkedList<>();
            q.add(first_node);
            visited_map.add(first_node.getId());

            while (!q.isEmpty()) {
                Node n = q.poll();

                for (Relationship rels : n.getRelationships(Direction.BOTH)) {
                    Node other_n = rels.getOtherNode(n);
                    if (!visited_map.contains(other_n.getId())) {
                        q.add(other_n);
                        visited_map.add(other_n.getId());
                    }
                }

            }
            tx.success();
        }

        boolean result = (visited_map.size() == numberofnodes);
        System.out.println(result ? "Connected" : "Not Connected" + "   at " + this.neo4j.DB_PATH);
        return result;

    }
}
