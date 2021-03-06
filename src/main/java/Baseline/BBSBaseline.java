package Baseline;

import DataStructure.Monitor;
import Neo4jTools.Line;
import Neo4jTools.Neo4jDB;
import org.apache.commons.cli.*;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class BBSBaseline {

    public double[] iniLowerBound;
    int graphsize = 2000;
    int degree = 4;
    int dimension = 3;
    //    HashMap<Long, HashMap<Long, myNode>> index = new HashMap<>(); //source node id ==> HashMap < destination node id, myNode objects that stores skyline paths>
    ArrayList<path> results = new ArrayList<>();
    Monitor monitor;
    private GraphDatabaseService graphdb;
    private Neo4jDB neo4j;


    public BBSBaseline() {
        String sub_db_name = graphsize + "_" + degree + "_" + dimension + "_Level" + 0;
        neo4j = new Neo4jDB(sub_db_name);
//        System.out.println(neo4j.DB_PATH);
        neo4j.startDB(true);
        graphdb = neo4j.graphDB;
        this.monitor = new Monitor();

    }

    public BBSBaseline(int graphsize, int degree, int dimension) {

        this.graphsize = graphsize;
        this.degree = degree;
        this.dimension = dimension;

        String sub_db_name = graphsize + "_" + degree + "_" + dimension + "_Level" + 0;
        neo4j = new Neo4jDB(sub_db_name);
        System.out.println(neo4j.DB_PATH);
        neo4j.startDB(true);
        graphdb = neo4j.graphDB;
        this.monitor = new Monitor();
    }

    public static void main(String args[]) {
        Options options = new Options();
        options.addOption("g", "grahpsize", true, "number of nodes in the graph");
        options.addOption("de", "degree", true, "degree of the graphe");
        options.addOption("di", "dimension", true, "dimension of the graph");
        options.addOption("h", "help", false, "print the help of this command");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        String g_str = cmd.getOptionValue("g");
        String de_str = cmd.getOptionValue("de");
        String di_str = cmd.getOptionValue("di");

        int numberNodes, numberofDegree, numberofDimen;

        if (cmd.hasOption("h")) {
            HelpFormatter formatter = new HelpFormatter();
            String header = "Run the code to build the indexes by using the basic bbs algorithm :";
            formatter.printHelp("java -jar BBSIndexBuilt.jar", header, options, "", false);
        } else {

            if (g_str == null) {
                numberNodes = 2000;
            } else {
                numberNodes = Integer.parseInt(g_str);
            }

            if (de_str == null) {
                numberofDegree = 4;
            } else {
                numberofDegree = Integer.parseInt(de_str);
            }

            if (di_str == null) {
                numberofDimen = 3;
            } else {
                numberofDimen = Integer.parseInt(di_str);
            }

            long start_time = System.currentTimeMillis();
            BBSBaseline baseline = new BBSBaseline(numberNodes, numberofDegree, numberofDimen);
            baseline.createIndexFolder();
            int overallskylinesize = 0;
            for (int i = 0; i < baseline.graphsize; i++) {
                int size = baseline.bbs(i);
                overallskylinesize += size;
                System.out.println("Finished the finding of the index of the node " + i + "   size of skyline paths : " + size);
            }
            System.out.println(overallskylinesize);
//            baseline.printSummurizationInformation();
            baseline.closeDB();
            System.out.println("The index built time is " + (System.currentTimeMillis() - start_time));
        }


    }

//    private void printSummurizationInformation() {
//        createIndexFolder();
//
//        long overall_summation = 0;
//        for (int nodeID = 0; nodeID < graphsize; nodeID++) {
//            long summation = 0;
//            HashMap<Long, myNode> destination_index = this.index.get((long) nodeID);
////            writeToDisk(destination_index, nodeID);
//
//            for (long i = 0; i < graphsize; i++) {
//                ArrayList<path> skys = destination_index.get(i).skyPaths;
//                long size = skys.size();
////                for (path p : skys) {
////                    System.out.println(nodeID + " " + i + " " + p.costs[0] + " " + p.costs[1] + " " + p.costs[2]);
////                }
//                summation += size;
//                System.out.println( nodeID + " to " + i + "  " + size);
//            }
//            System.out.println("the number of the skyline paths from  " + nodeID + " to others is " + summation);
//            overall_summation += summation;
//        }
//
//        System.out.println("the total index size is " + overall_summation + "/=" + (overall_summation / 2));
//    }

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

            for (Map.Entry<Long, myNode> e : destination_index.entrySet()) {
                long nodeid = e.getKey();
                myNode node_obj = e.getValue();
                ArrayList<path> skys = node_obj.skyPaths;
                for (path p : skys) {
                    writer.write(nodeid + " " + p.costs[0] + " " + p.costs[1] + " " + p.costs[2] + "\n");
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

    public int bbs(long nodeID) {
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


        int sum = 0;
        for (Map.Entry<Long, myNode> e : tmpStoreNodes.entrySet()) {
            int size = e.getValue().skyPaths.size();
            sum += size;
        }
        writeToDisk(tmpStoreNodes, nodeID);
        return sum;
    }

    public ArrayList<path> queryOnline(long src, long dest) {
        HashMap<Long, myNode> tmpStoreNodes = new HashMap();
        boolean haveResult = false;
        try (Transaction tx = this.graphdb.beginTx()) {
            myNode snode = new myNode(src, this.neo4j);
            myNodePriorityQueue mqueue = new myNodePriorityQueue();
            tmpStoreNodes.put(snode.id, snode);

            mqueue.add(snode);
            while (!mqueue.isEmpty()) {
                myNode v = mqueue.pop();
                for (int i = 0; i < v.skyPaths.size(); i++) {
                    path p = v.skyPaths.get(i);
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

                            if (np.endNode == dest) {
                                addToSkyline(np);
                                if(!haveResult){
                                    haveResult=true;
                                    long nodes_add_skyline_when_have_one_result = 0;
                                    long nodes_covered_when_have_result = 0 ;
                                    for (Map.Entry<Long, myNode> e : tmpStoreNodes.entrySet()) {
                                        nodes_add_skyline_when_have_one_result += e.getValue().callAddToSkylineFunction;
                                        if (!e.getValue().skyPaths.isEmpty()) {
                                            nodes_covered_when_have_result++;
                                        }
                                    }
                                    System.out.println("    "+monitor.callcheckdominatedbyresult+" "+nodes_add_skyline_when_have_one_result+" "+nodes_covered_when_have_result);

                                }
                            } else if (!dominatedByResult(np)) {
                                if (next_n.addToSkyline(np) && !next_n.inqueue) {
                                    mqueue.add(next_n);
                                    next_n.inqueue = true;
                                }
                            }
                        }
                    }
                }

            }

            tx.success();
        }

        for (Map.Entry<Long, myNode> e : tmpStoreNodes.entrySet()) {
            this.monitor.node_call_addtoskyline += e.getValue().callAddToSkylineFunction;
            if (!e.getValue().skyPaths.isEmpty()) {
                this.monitor.coveredNodes++;
            }
        }

        return tmpStoreNodes.get(dest).skyPaths;

    }

    private boolean dominatedByResult(path np) {
        monitor.callcheckdominatedbyresult++;
        monitor.allsizeofthecheckdominatedbyresult += this.results.size();
        long rt_check_dominatedByresult = System.nanoTime();
        for (path rp : results) {
            if (checkDominated(rp.costs, np.costs)) {
                long rt_check_dominatedByresult_endwithTrue = System.nanoTime();
                monitor.runningtime_check_domination_result += (rt_check_dominatedByresult_endwithTrue - rt_check_dominatedByresult);
                return true;
            }
        }

        long rt_check_dominatedByresult_endwithFalse = System.nanoTime();
        monitor.runningtime_check_domination_result += (rt_check_dominatedByresult_endwithFalse - rt_check_dominatedByresult);
        return false;
    }

    public void initilizeSkylinePath(long srcNode, long destNode) {
        int i = 0;

        this.iniLowerBound = new double[this.dimension];

        try (Transaction tx = this.neo4j.graphDB.beginTx()) {
            Node destination = this.neo4j.graphDB.getNodeById(destNode);
            Node startNode = this.neo4j.graphDB.getNodeById(srcNode);


            for (String property_name : Neo4jDB.propertiesName) {
                PathFinder<WeightedPath> finder = GraphAlgoFactory
                        .dijkstra(PathExpanders.forTypeAndDirection(Line.Linked, Direction.BOTH), property_name);
                WeightedPath paths = finder.findSinglePath(startNode, destination);
                if (paths != null) {
                    path np = new path(paths);
                    this.iniLowerBound[i++] = paths.weight();
                    addToSkyline(np);
                }
            }
            tx.success();
        }
    }


    public boolean addToSkyline(path np) {
        this.monitor.callAddToSkyline++;
        int i = 0;
        if (results.isEmpty()) {
            this.results.add(np);
            return true;
        } else {
            boolean can_insert_np = true;
            for (; i < results.size(); ) {
                if (checkDominated(results.get(i).costs, np.costs)) {
                    can_insert_np = false;
                    break;
                } else {
                    if (checkDominated(np.costs, results.get(i).costs)) {
                        this.results.remove(i);
                    } else {
                        i++;
                    }
                }
            }

            if (can_insert_np) {
                this.results.add(np);
                return true;
            }
        }
        return false;
    }

    /**
     * if all the costs of the target path is less than the costs of the wanted path, means target path dominate the wanted path
     *
     * @param costs          the target path
     * @param estimatedCosts the wanted path
     * @return if the target path dominates the wanted path, return true. other wise return false.
     */
    private boolean checkDominated(double[] costs, double[] estimatedCosts) {
        for (int i = 0; i < costs.length; i++) {
            if (costs[i] * (1) > estimatedCosts[i]) {
                return false;
            }
        }
        return true;
    }
}
