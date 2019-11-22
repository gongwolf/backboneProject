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
import java.util.Random;

public class BBSBaselineBusline {

    int graphsize = 2000;
    double samet = 4;
    int level = 3;
    //    HashMap<Long, HashMap<Long, myNode>> index = new HashMap<>(); //source node id ==> HashMap < destination node id, myNode objects that stores skyline paths>
    ArrayList<path> results = new ArrayList<>();
    Monitor monitor;
    private GraphDatabaseService graphdb;
    public Neo4jDB neo4j;

    /**
     * landmark nodes --> <dest nodes, <the value of shortest path from landmark nodes to dest nodes in each dimension>>
     **/
    public HashMap<Long, HashMap<Long, double[]>> landmark_index = new HashMap<>();


    public BBSBaselineBusline(int graphsize, double samet, int level) {
        this.graphsize = graphsize;
        this.samet = samet;
        this.level = level;
        String sub_db_name = graphsize + "_" + samet + "_Level" + level;
        neo4j = new Neo4jDB(sub_db_name);
        System.out.println(neo4j.DB_PATH);
        neo4j.startDB(true);
        graphdb = neo4j.graphDB;
        this.monitor = new Monitor();
    }

    public void buildLandmarkIndex(int num_landmarks) {
        this.landmark_index.clear();

        try (Transaction tx = this.neo4j.graphDB.beginTx()) {

            ArrayList<Node> nodelist = new ArrayList<>();

            ResourceIterable<Node> nodes_iterable = this.neo4j.graphDB.getAllNodes();
            ResourceIterator<Node> nodes_iter = nodes_iterable.iterator();
            while (nodes_iter.hasNext()) {
                Node node_id = nodes_iter.next();
                nodelist.add(node_id);
            }


            ArrayList<Node> landmarks = new ArrayList<>();
            while (landmarks.size() < num_landmarks) {
                Node landmarks_node = getRandomNodes(nodelist);
                if (!landmarks.contains(landmarks_node)) {
                    landmarks.add(landmarks_node);
                }
            }

            for (Node lnode : landmarks) {
                HashMap<Long, double[]> index_from_landmark_to_dest = new HashMap<>();
                System.out.println(lnode);

                int index = 0;

                for (Node destination : nodelist) {
                    if ((++index) % 500 == 0) {
                        System.out.println(lnode + "    " + index + " ..............................");
                    }


                    if (destination.getId() == lnode.getId()) {
                        index_from_landmark_to_dest.put(destination.getId(), new double[3]);
                        continue;
                    }

                    int i = 0;
                    double[] min_costs = new double[3];
                    for (String property_name : Neo4jDB.propertiesName) {
                        PathFinder<WeightedPath> finder = GraphAlgoFactory
                                .dijkstra(PathExpanders.forTypeAndDirection(Line.Linked, Direction.BOTH), property_name);
                        WeightedPath paths = finder.findSinglePath(lnode, destination);
                        if (paths != null) {
                            path np = new path(paths);
                            min_costs[i++] = paths.weight();
                        }
                    }
                    index_from_landmark_to_dest.put(destination.getId(), min_costs);
                }

                this.landmark_index.put(lnode.getId(), index_from_landmark_to_dest);
            }

            tx.success();
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
        return sum;
    }

    public ArrayList<path> queryOnline(long src, long dest) {
        HashMap<Long, myNode> tmpStoreNodes = new HashMap();
        this.results.clear();
        this.monitor = new Monitor();


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

                    if (landmark_index.size() != 0) {
                        double[] p_l_costs = getLowerBound(p.costs, src, dest);
                        if (dominatedByResult(p_l_costs)) {
                            continue;
                        }
                    }

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

//                            if(np.hasCycle()){
//                                continue;
//                            }

                            if (np.endNode == dest) {
                                addToSkyline(np);
                                if (!haveResult) {
                                    haveResult = true;
                                    //number of skyline of each node are added before the first final result are insert
                                    long nodes_add_skyline_when_have_one_result = 0;
                                    //number of nodes are visited before the first final result are insert
                                    long nodes_covered_when_have_result = 0;
                                    for (Map.Entry<Long, myNode> e : tmpStoreNodes.entrySet()) {
                                        nodes_add_skyline_when_have_one_result += e.getValue().callAddToSkylineFunction;
                                        if (!e.getValue().skyPaths.isEmpty()) {
                                            nodes_covered_when_have_result++;
                                        }
                                    }
                                    System.out.println("    " + monitor.callcheckdominatedbyresult + " " + nodes_add_skyline_when_have_one_result + " " + nodes_covered_when_have_result);
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

    private double[] getLowerBound(double[] costs, long src, long dest) {
        double[] estimated_costs = new double[3];

        for (int i = 0; i < estimated_costs.length; i++) {
            estimated_costs[i] = Double.NEGATIVE_INFINITY;
        }

        for (Long landmark : this.landmark_index.keySet()) {
            double[] src_cost = this.landmark_index.get(landmark).get(src);
            double[] dest_cost = this.landmark_index.get(landmark).get(dest);
            for (int i = 0; i < estimated_costs.length; i++) {
                double value = Math.abs(src_cost[i] - dest_cost[i]);
                if (value > estimated_costs[i]) {
                    estimated_costs[i] = value;
                }
            }
        }

        for (int i = 0; i < estimated_costs.length; i++) {
            estimated_costs[i] += costs[i];
        }

        return estimated_costs;
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

    private boolean dominatedByResult(double estimated_costs[]) {
        monitor.callcheckdominatedbyresult++;
        monitor.allsizeofthecheckdominatedbyresult += this.results.size();
        long rt_check_dominatedByresult = System.nanoTime();
        for (path rp : results) {
            if (checkDominated(rp.costs, estimated_costs)) {
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

//        this.iniLowerBound = new double[3];

        try (Transaction tx = this.neo4j.graphDB.beginTx()) {
            Node destination = this.neo4j.graphDB.getNodeById(destNode);
            Node startNode = this.neo4j.graphDB.getNodeById(srcNode);


            for (String property_name : Neo4jDB.propertiesName) {
                PathFinder<WeightedPath> finder = GraphAlgoFactory
                        .dijkstra(PathExpanders.forTypeAndDirection(Line.Linked, Direction.BOTH), property_name);
                WeightedPath paths = finder.findSinglePath(startNode, destination);
                if (paths != null) {
                    path np = new path(paths);
//                    this.iniLowerBound[i++] = paths.weight();
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
     * if all the costs of the target path is less than the estimated costs of the wanted path, means target path dominate the wanted path
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


    private <T> T getRandomNodes(ArrayList<T> nodelist) {
        Random r = new Random();
        int idx = r.nextInt(nodelist.size());
        return nodelist.get(idx);
    }
}
