package Baseline;

import Neo4jTools.Neo4jDB;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class BBSBaseline {

    int graphsize = 1000;
    int degree = 4;
    int dimension = 3;
    HashMap<Long, HashMap<Long, myNode>> index = new HashMap<>();
    private GraphDatabaseService graphdb;
    private Neo4jDB neo4j;


    public BBSBaseline() {
        String sub_db_name = graphsize + "_" + degree + "_" + dimension + "_Level" + 0;
        neo4j = new Neo4jDB(sub_db_name);
//        System.out.println(neo4j.DB_PATH);
        neo4j.startDB(true);
        graphdb = neo4j.graphDB;
    }

    public static void main(String args[]) {
        BBSBaseline baseline = new BBSBaseline();
        for (int i = 0; i < baseline.graphsize; i++) {
            baseline.bbs(i);
//            System.out.println("Finished the finding of the index of the node " + i);
        }
        baseline.printSummurizationInformation();
        baseline.closeDB();
    }

    private void printSummurizationInformation() {
        createIndexFolder();

        long overall_summation = 0;
        for (int nodeID = 0; nodeID < graphsize; nodeID++) {
            long summation = 0;
            HashMap<Long, myNode> destination_index = this.index.get((long) nodeID);
            writeToDisk(destination_index, nodeID);

            for (long i = 0; i < 1000; i++) {
                ArrayList<path> skys = destination_index.get(i).skyPaths;
                long size = skys.size();
//                for (path p : skys) {
//                    System.out.println(nodeID + " " + i + " " + p.costs[0] + " " + p.costs[1] + " " + p.costs[2]);
//                }
                summation += size;
//                System.out.println(nodeID + " to " + i + "  " + size);
            }
            System.out.println("the number of the skyline paths from  " + nodeID + " to others is " + summation);
            overall_summation += summation;
        }
        System.out.println("the total index size is " + overall_summation + "/=" + (overall_summation / 2));
    }

    private void createIndexFolder() {
        String folder = "/home/gqxwolf/mydata/projectData/BackBone/indexes/baseline_" + graphsize + "_" + degree + "_" + dimension;
        File idx_folder = new File(folder);
        if (idx_folder.exists()) {
            idx_folder.delete();
        }

        idx_folder.mkdirs();

    }

    private void writeToDisk(HashMap<Long, myNode> destination_index, long nodeID) {
        BufferedWriter writer = null;
        try {

            String file_path = "/home/gqxwolf/mydata/projectData/BackBone/indexes/baseline_" + graphsize + "_" + degree + "_" + dimension + "/" + nodeID + ".idx";

            File idx_file = new File(file_path);
            if (idx_file.exists()) {
                idx_file.delete();
            }

            writer = new BufferedWriter(new FileWriter(file_path));

            for (long i = 0; i < 1000; i++) {
                ArrayList<path> skys = destination_index.get(i).skyPaths;
                for (path p : skys) {
                    writer.write(i + " " + p.costs[0] + " " + p.costs[1] + " " + p.costs[2]+"\n");
                }
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void closeDB() {
        if (neo4j != null) {
//            System.out.println(neo4j.DB_PATH + " is closed successfully");
            this.neo4j.closeDB();
        }
    }

    public void bbs(long nodeID) {
        HashMap<Long, myNode> tmpStoreNodes = new HashMap();

        try (Transaction tx = this.graphdb.beginTx()) {
            myNode snode = new myNode(nodeID, this.neo4j);
            myNodePriorityQueue mqueue = new myNodePriorityQueue();
            tmpStoreNodes.put(snode.id, snode);

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
                            myNode next_n;
                            if (tmpStoreNodes.containsKey(np.endNode)) {
                                next_n = tmpStoreNodes.get(np.endNode);
                            } else {
                                next_n = new myNode(snode, np.endNode, neo4j);
                                tmpStoreNodes.put(next_n.id, next_n);
                            }


                            if (next_n.addToSkyline(np) && !next_n.inqueue) {
                                mqueue.add(next_n);
                                next_n.inqueue = true;
                            }

                        }
                    }
                }

            }

            tx.success();
        }

        this.index.put(nodeID, tmpStoreNodes);
    }
}
