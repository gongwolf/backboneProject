package Neo4jTools;

import org.neo4j.graphdb.*;

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
        long start_time = System.currentTimeMillis();
        long expansion_time = 0;
        long mynode_finding_time = 0;
        long addtoskyline_time = 0;
        long overall_skyline_size = 0 ;


        String graph_db_folder = this.base_db_name + "_Level" + 8;

        Neo4jDB neo4j_level = new Neo4jDB(graph_db_folder);
        neo4j_level.startDB(true);
        GraphDatabaseService graphdb_level = neo4j_level.graphDB;
        System.out.println("number of nodes : " + neo4j_level.getNumberofNodes() + "  number of edges: " + neo4j_level.getNumberofEdges() + " ---->>>> " + neo4j_level.graphDB);

        try (Transaction tx = graphdb_level.beginTx()) {
            ResourceIterable<Node> allnodes_iteratable = graphdb_level.getAllNodes();
            ResourceIterator<Node> allnodes_iter = allnodes_iteratable.iterator();

            int counter=1;
            while (allnodes_iter.hasNext()) {
                long start_time_iter = System.currentTimeMillis();
                HashMap<Long, myNode> tmpStoreNodes = new HashMap();
                Node node = allnodes_iter.next();
                long nodeID = node.getId();
                System.out.print(counter++ +" process_node:" + nodeID);
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

                            long expansion_time_start = System.nanoTime();
                            ArrayList<path> new_paths = p.expand(neo4j_level);
                            expansion_time += System.nanoTime() - expansion_time_start;

                            for (path np : new_paths) {
                                myNode next_n;
                                long mynode_finding_time_start = System.nanoTime();
                                if (tmpStoreNodes.containsKey(np.endNode)) {
                                    next_n = tmpStoreNodes.get(np.endNode);
                                } else {
                                    next_n = new myNode(snode, np.endNode, neo4j_level);
                                    tmpStoreNodes.put(next_n.id, next_n);
                                }
                                mynode_finding_time += System.nanoTime() - mynode_finding_time_start;

                                long addtoskyline_time_start = System.nanoTime();
                                boolean add_succ = next_n.addToSkyline(np);
                                addtoskyline_time += System.nanoTime() - addtoskyline_time_start;

                                if (add_succ && !next_n.inqueue) {
                                    mqueue.add(next_n);
                                    next_n.inqueue = true;
                                }
                            }
                        }
                    }
                }

//                System.out.println("Finished find the skyline paths for node   " + nodeID + " " + (System.currentTimeMillis() - start_time));

                long number_addtoskyline = 0;
                long number_of_skyline = 0;
                for (Map.Entry<Long, myNode> e : tmpStoreNodes.entrySet()) {
                    number_addtoskyline += e.getValue().callAddToSkylineFunction;
//                    System.out.println(e.getKey()+"   "+e.getValue().skyPaths.size());
                    number_of_skyline += e.getValue().skyPaths.size();
                }
                overall_skyline_size+=number_of_skyline;

                System.out.print("   call_add_to_skyline_function:" + number_addtoskyline);
                System.out.print("  Running_time:"+(System.currentTimeMillis() - start_time_iter));
                System.out.println("    number_of_total_skyline_paths:" + number_of_skyline+"  overall:"+overall_skyline_size);
                tmpStoreNodes.clear();
//                break;
            }
            System.out.println("==================================================================================");
            tx.success();
            System.out.println("Running time (without close db): "+(System.currentTimeMillis() - start_time)+" ms");
        }
        neo4j_level.closeDB();
        System.out.println("Running time : "+(System.currentTimeMillis() - start_time)+" ms");
        System.out.println("====================================================");
        System.out.println(expansion_time / 1000000);
        System.out.println(mynode_finding_time / 1000000);
        System.out.println(addtoskyline_time / 1000000);
        System.out.println("Overall skyline size at this level: "+overall_skyline_size);
    }


    private void bbsatlevel(long node_id) {
        long start_time = System.currentTimeMillis();
        long expansion_time = 0;
        long mynode_finding_time = 0;
        long addtoskyline_time = 0;


        String graph_db_folder = this.base_db_name + "_Level" + 9;

        Neo4jDB neo4j_level = new Neo4jDB(graph_db_folder);
        neo4j_level.startDB(true);
        GraphDatabaseService graphdb_level = neo4j_level.graphDB;
        System.out.println("number of nodes : " + neo4j_level.getNumberofNodes() + "  number of edges: " + neo4j_level.getNumberofEdges() + " ---->>>> " + neo4j_level.graphDB);

        try (Transaction tx = graphdb_level.beginTx()) {
            HashMap<Long, myNode> tmpStoreNodes = new HashMap();
            Node node = graphdb_level.getNodeById(node_id);
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

                        long expansion_time_start = System.nanoTime();
                        ArrayList<path> new_paths = p.expand(neo4j_level);
                        expansion_time += System.nanoTime() - expansion_time_start;

                        for (path np : new_paths) {
                            myNode next_n;
                            long mynode_finding_time_start = System.nanoTime();
                            if (tmpStoreNodes.containsKey(np.endNode)) {
                                next_n = tmpStoreNodes.get(np.endNode);
                            } else {
                                next_n = new myNode(snode, np.endNode, neo4j_level);
                                tmpStoreNodes.put(next_n.id, next_n);
                            }
                            mynode_finding_time += System.nanoTime() - mynode_finding_time_start;

                            long addtoskyline_time_start = System.nanoTime();
                            boolean add_succ = next_n.addToSkyline(np);
                            addtoskyline_time += System.nanoTime() - addtoskyline_time_start;

                            if (add_succ && !next_n.inqueue) {
                                mqueue.add(next_n);
                                next_n.inqueue = true;
                            }
                        }
                    }
                }
            }

            System.out.println("Finished find the skyline paths for node   " + nodeID + " " + (System.currentTimeMillis() - start_time));

            long number_addtoskyline = 0;
            StringBuffer sb = new StringBuffer();
            sb.append("[");
            for (Map.Entry<Long, myNode> e : tmpStoreNodes.entrySet()) {
                number_addtoskyline += e.getValue().callAddToSkylineFunction;
                if (e.getValue().skyPaths.size() == 3338) {
                    sb.append(e.getKey()).append(",");
                }
                System.out.println(e.getKey() + "   " + e.getValue().skyPaths.size());
            }
            sb.append("]");
            System.out.println("call add to skyline function " + number_addtoskyline);
            System.out.println(sb);

            tx.success();
            System.out.println((System.currentTimeMillis() - start_time));

        }
        neo4j_level.closeDB();
        System.out.println((System.currentTimeMillis() - start_time));
        System.out.println("====================================================");
        System.out.println(expansion_time / 1000000);
        System.out.println(mynode_finding_time / 1000000);
        System.out.println(addtoskyline_time / 1000000);

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
