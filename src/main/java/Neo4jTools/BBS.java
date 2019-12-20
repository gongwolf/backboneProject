package Neo4jTools;

import org.neo4j.graphdb.*;
import v4.myNode;
import v4.myNodePriorityQueue;
import v4.path;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class BBS {
    public String base_db_name = "sub_ny_USA";


    public static void main(String args[]) {
        BBS bbs = new BBS();
//        bbs.checkatlevel();
        bbs.bbsatlevel();
    }

    private void bbsatlevel() {
        String graph_db_folder = this.base_db_name + "_Level" + 6;

        Neo4jDB neo4j_level = new Neo4jDB(graph_db_folder);
        neo4j_level.startDB(true);
        GraphDatabaseService graphdb_level = neo4j_level.graphDB;
        System.out.println("number of nodes : " + neo4j_level.getNumberofNodes() + "  " + neo4j_level.graphDB);

        try (Transaction tx = graphdb_level.beginTx()) {
            ResourceIterable<Node> allnodes_iteratable = graphdb_level.getAllNodes();
            ResourceIterator<Node> allnodes_iter = allnodes_iteratable.iterator();

            BufferedWriter writer = null;

            while (allnodes_iter.hasNext()) {
                HashMap<Long, myNode> tmpStoreNodes = new HashMap();
                Node node = allnodes_iter.next();
                long nodeID = node.getId();
                System.out.println("process node :" + nodeID);
                myNode snode = new myNode(nodeID, neo4j_level);
                myNodePriorityQueue mqueue = new myNodePriorityQueue();
                tmpStoreNodes.put(snode.id, snode);
                mqueue.add(snode);
                while (!mqueue.isEmpty()) {
                    myNode v = mqueue.pop();
                    for (int i = 0; i < v.skyPaths.size(); i++) {
                        path p = v.skyPaths.get(i);
                        if (!p.expaned) {
                            p.expaned = true;
                            ArrayList<path> new_paths = p.expand(neo4j_level);
                            for (path np : new_paths) {
//                                    System.out.println("    " + np);
                                myNode next_n;
                                if (tmpStoreNodes.containsKey(np.endNode)) {
                                    next_n = tmpStoreNodes.get(np.endNode);
                                } else {
                                    next_n = new myNode(snode, np.endNode, neo4j_level);
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
                System.out.println("Finished find the skyline paths for node   " + nodeID);
            }
            tx.success();
        }

        neo4j_level.closeDB();

    }

    private void checkatlevel() {
        int graphsize = 10000;
        double samenode_t = 2.844;
        double percentage = 0.1;
        int level = 7;

        String graph_db_folder = this.base_db_name + "_Level" + level;

        Neo4jDB neo4j_level = new Neo4jDB(graph_db_folder);
        neo4j_level.startDB(true);
        GraphDatabaseService graphdb_level = neo4j_level.graphDB;
        System.out.println("number of nodes : " + neo4j_level.getNumberofNodes());

        long numIndex = 0;
        long sizeOverallSkyline = 0;

        String sub_folder_str = "/home/gqxwolf/mydata/projectData/BackBone/indexes/backbone_" + graphsize + "_" + samenode_t + "_backup_" + percentage + "/level" + level;


        try (Transaction tx = graphdb_level.beginTx()) {
            ResourceIterable<Node> allnodes_iteratable = graphdb_level.getAllNodes();
            ResourceIterator<Node> allnodes_iter = allnodes_iteratable.iterator();

            BufferedWriter writer = null;

            while (allnodes_iter.hasNext()) {
                HashMap<Long, myNode> tmpStoreNodes = new HashMap();
                Node node = allnodes_iter.next();
                long nodeID = node.getId();
                System.out.println("process node :" + nodeID);
                myNode snode = new myNode(nodeID, neo4j_level);
                myNodePriorityQueue mqueue = new myNodePriorityQueue();
                tmpStoreNodes.put(snode.id, snode);
                mqueue.add(snode);
                while (!mqueue.isEmpty()) {
                    myNode v = mqueue.pop();
                    for (int i = 0; i < v.skyPaths.size(); i++) {
                        path p = v.skyPaths.get(i);
                        if (!p.expaned) {
                            p.expaned = true;
                            ArrayList<path> new_paths = p.expand(neo4j_level);
                            for (path np : new_paths) {
//                                    System.out.println("    " + np);
                                myNode next_n;
                                if (tmpStoreNodes.containsKey(np.endNode)) {
                                    next_n = tmpStoreNodes.get(np.endNode);
                                } else {
                                    next_n = new myNode(snode, np.endNode, neo4j_level);
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
                System.out.println("Finished find the skyline paths for node   " + nodeID);

                int sum = 0;
                for (Map.Entry<Long, myNode> e : tmpStoreNodes.entrySet()) {
                    ArrayList<path> sk = e.getValue().skyPaths;
                    //remove the index of the self connection that node only has one skyline path and the skyline path is to itself
                    if (!(sk.size() == 1 && sk.get(0).costs[0] == 0 && sk.get(0).costs[1] == 0 && sk.get(0).costs[2] == 0)) {
                        sum = 1;
                    }
                }

                sizeOverallSkyline += sum;

                if (sum != 0) {
                    numIndex++;
                    /**clean the built index file*/
                    File idx_file = new File(sub_folder_str + "/" + nodeID + ".idx");
                    if (idx_file.exists()) {
                        idx_file.delete();
                    }

                    writer = new BufferedWriter(new FileWriter(idx_file.getAbsolutePath()));
                    for (Map.Entry<Long, myNode> e : tmpStoreNodes.entrySet()) {
                        long nodeid = e.getKey();
                        myNode node_obj = e.getValue();
                        ArrayList<path> skys = node_obj.skyPaths;
                        for (path p : skys) {
                            /** the end node of path is a highway, the node is still appear in next level, also, the path is not a dummy path of source node **/
                            if (p.endNode != nodeID) {
                                writer.write(nodeid + " " + p.costs[0] + " " + p.costs[1] + " " + p.costs[2] + "\n");
                            }
                        }
                    }
                    writer.close();
                }
            }
            tx.success();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
