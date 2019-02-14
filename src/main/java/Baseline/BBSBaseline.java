package Baseline;

import Neo4jTools.Neo4jDB;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import java.util.ArrayList;
import java.util.HashMap;

public class BBSBaseline {

    int graphsize = 1000;
    int degree = 4;
    int dimension = 3;
    private GraphDatabaseService graphdb;
    private Neo4jDB neo4j;
    private HashMap<Long, myNode> tmpStoreNodes = new HashMap();


    public BBSBaseline() {
        String sub_db_name = graphsize + "_" + degree + "_" + dimension + "_Level" + 0;
        neo4j = new Neo4jDB(sub_db_name);
        System.out.println(neo4j.DB_PATH);
        neo4j.startDB();
        graphdb = neo4j.graphDB;
    }

    public static void main(String args[]) {
        BBSBaseline baseline = new BBSBaseline();
        baseline.bbs();

    }

    public void closeDB() {
        if (neo4j != null) {
            System.out.println(neo4j.DB_PATH + " is closed successfully");
            this.neo4j.closeDB();
        }
    }

    public void bbs() {
        try (Transaction tx = this.graphdb.beginTx()) {
            myNode snode = new myNode(0, this.neo4j);
            myNodePriorityQueue mqueue = new myNodePriorityQueue();
            this.tmpStoreNodes.put(snode.id, snode);

            mqueue.add(snode);
            while (!mqueue.isEmpty()) {
                myNode v = mqueue.pop();
                for (int i = 0; i < v.skyPaths.size(); i++) {
                    path p = v.skyPaths.get(i);
//                    System.out.println(p);
//                    ArrayList<path> new_paths = p.expand(neo4j);
//                    for (path np : new_paths) {
//                        System.out.println("    "+np);
//                    }
                    if (!p.expaned) {
                        p.expaned = true;
                        ArrayList<path> new_paths = p.expand(neo4j);
                        for (path np : new_paths) {
                            System.out.println(np);
                            myNode next_n;
                            if (this.tmpStoreNodes.containsKey(np.endNode)) {
                                next_n = tmpStoreNodes.get(np.endNode);
                            } else {
                                next_n = new myNode(snode, np.endNode, neo4j);
                                this.tmpStoreNodes.put(next_n.id, next_n);
                            }


                            if (next_n.addToSkyline(np) && !next_n.inqueue) {
                                mqueue.add(next_n);
                                next_n.inqueue = true;
                            }

                        }
                    }
                }

            }

            long summation = 0;
            for (int i = 0; i < 1000; i++) {
                long size = tmpStoreNodes.get((long) i).skyPaths.size();
                summation += size;
                System.out.println("0 to " + i + "  " + size);
            }
            System.out.println("summation " + summation);
        }
        closeDB();
    }
}
