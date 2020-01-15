package Baseline;

import DataStructure.Monitor;
import Neo4jTools.Line;
import Neo4jTools.Neo4jDB;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.*;


import java.util.*;

public class BBSBaselineBusline {

    int graphsize = 2000;
    double samet = 4;
    int level = 3;
    //    HashMap<Long, HashMap<Long, myNode>> index = new HashMap<>(); //source node id ==> HashMap < destination node id, myNode objects that stores skyline paths>
    ArrayList<path> results = new ArrayList<>();
    Monitor monitor;
    private GraphDatabaseService graphdb;
    public Neo4jDB neo4j;
    public HashSet<Long> node_list = new HashSet<>();

    /**
     * landmark nodes --> <dest nodes, <the value of shortest path from landmark nodes to dest nodes in each dimension>>
     **/
    public HashMap<Long, HashMap<Long, double[]>> landmark_index = new HashMap<>();


    public static void main(String args[]) {
        BBSBaselineBusline bbs = new BBSBaselineBusline();
//        int number_of_hops_1 = bbs.findShortestPath(3227l, 8222l, Neo4jDB.propertiesName.get(0)).length();
//        int number_of_hops_2 = bbs.findShortestPath(3227l, 8222l, Neo4jDB.propertiesName.get(1)).length();
//        int number_of_hops_3 = bbs.findShortestPath(3227l, 8222l, Neo4jDB.propertiesName.get(2)).length();
//        System.out.println(Neo4jDB.propertiesName.get(0)+"  "+number_of_hops_1+" | "+Neo4jDB.propertiesName.get(1)+"  "+number_of_hops_2+" | "+Neo4jDB.propertiesName.get(2)+"  "+number_of_hops_3+" ");
        bbs.buildLandmarkIndex(3);
        long start_rt = System.currentTimeMillis();
        ArrayList<path> results = bbs.queryOnline(3227, 8222);
        System.out.println(results.size() + "   " + (System.currentTimeMillis() - start_rt));
        bbs.closeDB();

    }

    private WeightedPath findShortestPath(long src, long dest, String property_name) {
        WeightedPath paths;
        try (Transaction tx = graphdb.beginTx()) {
            Node src_node = graphdb.getNodeById(src);
            Node dest_node = graphdb.getNodeById(dest);
            PathFinder<WeightedPath> finder = GraphAlgoFactory
                    .dijkstra(PathExpanders.forTypeAndDirection(Line.Linked, Direction.BOTH), property_name);
            paths = finder.findSinglePath(src_node, dest_node);
        }
        return paths;
    }

    public BBSBaselineBusline(int graphsize, double samet, int level) {
        this.graphsize = graphsize;
        this.samet = samet;
        this.level = level;
        String sub_db_name = graphsize + "_" + samet + "_Level" + level;
//        String sub_db_name = "sub_ny_USA_level0";
        neo4j = new Neo4jDB(sub_db_name);
        System.out.println(neo4j.DB_PATH);
        neo4j.startDB(true);
        graphdb = neo4j.graphDB;
        this.monitor = new Monitor();
    }

    public BBSBaselineBusline() {
        String sub_db_name = "sub_ny_USA_Level0";
        neo4j = new Neo4jDB(sub_db_name);
        neo4j.startDB(true);
        graphdb = neo4j.graphDB;
        System.out.println(neo4j.DB_PATH + "  number of nodes:" + neo4j.getNumberofNodes() + "   number of edges : " + neo4j.getNumberofEdges());
        this.monitor = new Monitor();
    }


    public BBSBaselineBusline(String db_name) {
        String sub_db_name = db_name;
        neo4j = new Neo4jDB(sub_db_name);
        neo4j.startDB(true);
        graphdb = neo4j.graphDB;
        System.out.println(neo4j.DB_PATH + "  number of nodes:" + neo4j.getNumberofNodes() + "   number of edges : " + neo4j.getNumberofEdges());

        this.node_list = this.neo4j.getNodes();

        this.monitor = new Monitor();
    }

    public void buildLandmarkIndex(int num_landmarks) {
        this.landmark_index.clear();

        try (Transaction tx = this.neo4j.graphDB.beginTx()) {

            ArrayList<Node> nodelist = new ArrayList<>();

            ResourceIterable<Node> nodes_iterable = this.neo4j.graphDB.getAllNodes();
            ResourceIterator<Node> nodes_iter = nodes_iterable.iterator();
            while (nodes_iter.hasNext()) {
                Node node = nodes_iter.next();
                nodelist.add(node);
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
                System.out.println("Build the index for the node " + lnode);
                int index = 0;

                for (Node destination : nodelist) {
                    if ((++index) % 500 == 0) {
                        System.out.println(lnode + "    " + index + " ..............................");
                    }

                    int i = 0;
                    double[] min_costs = new double[3];
                    for (String property_name : Neo4jDB.propertiesName) {
                        PathFinder<WeightedPath> finder = GraphAlgoFactory
                                .dijkstra(PathExpanders.forTypeAndDirection(Line.Linked, Direction.BOTH), property_name);
                        WeightedPath paths = finder.findSinglePath(lnode, destination);
                        if (paths != null) {
                            min_costs[i] = paths.weight();
                            i++;
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


        long quer_running_time = System.nanoTime();

        long addtoskyline_rt = 0;
        long number_addtoskyline = 0;
        long upperbound_find_rt = 0;
        long check_dominate_result_rt = 0;

        try (Transaction tx = this.graphdb.beginTx()) {
            myNode snode = new myNode(src, neo4j);
            myNodePriorityQueue mqueue = new myNodePriorityQueue();
            tmpStoreNodes.put(snode.id, snode);
            mqueue.add(snode);
            snode.inqueue = false;

            while (!mqueue.isEmpty()) {
                myNode v = mqueue.pop();
                v.inqueue = false;
                for (int i = 0; i < v.skyPaths.size(); i++) {
                    path p = v.skyPaths.get(i);

                    boolean isDominatedByResult = false;
                    if (landmark_index.size() != 0) {

                        long upperbound_find_rt_start = System.nanoTime();
                        double[] p_l_costs = getLowerBound(p.costs, p.endNode, dest);
                        upperbound_find_rt += System.nanoTime() - upperbound_find_rt_start;


                        long dominate_rt_start = System.nanoTime();
                        if (dominatedByResult(p_l_costs)) {
                            isDominatedByResult = true;
                        }
                        check_dominate_result_rt += System.nanoTime() - dominate_rt_start;
                    }

                    if (isDominatedByResult) {
                        continue;
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

                            if (np.hasCycle()) {
                                continue;
                            }

                            boolean dominatebyresult = false;
//                            long dominate_rt_start = System.nanoTime();
//                            boolean dominatebyresult = dominatedByResult(np);
//                            check_dominate_result_rt += System.nanoTime() - dominate_rt_start;

                            if (np.endNode == dest) {
                                long addtoskyline_start = System.nanoTime();
                                addToSkyline(np);
                                addtoskyline_rt += System.nanoTime() - addtoskyline_start;
                            } else if (!dominatebyresult) {
                                long addtoskyline_start = System.nanoTime();
                                boolean add_succ = next_n.addToSkyline(np);
                                addtoskyline_rt += System.nanoTime() - addtoskyline_start;

                                if (add_succ && !next_n.inqueue) {
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

        System.out.println("Query time : " + (System.nanoTime() - quer_running_time) / 1000000);

        for (Map.Entry<Long, myNode> e : tmpStoreNodes.entrySet()) {
            number_addtoskyline += e.getValue().callAddToSkylineFunction;
        }

        System.out.println("add to skyline running time : "+addtoskyline_rt / 1000000);
        System.out.println("check domination by result time : "+check_dominate_result_rt / 1000000);
        System.out.println("upperbound calculation time  : "+upperbound_find_rt / 1000000);
        System.out.println("# of time to add to skyline function : "+ number_addtoskyline);

        return results;

    }

    private double[] getLowerBound(double[] costs, long src, long dest) {
        double[] estimated_costs = new double[3];

        for (int i = 0; i < estimated_costs.length; i++) {
            estimated_costs[i] = Double.NEGATIVE_INFINITY;
        }

        for (long landmark : this.landmark_index.keySet()) {
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


    public double myDijkstra(long sid, long did, String property_type) {

        HashMap<Long, myDijNode> nodelist = new HashMap<>(); //

        myNodeDijkstraPriorityQueue dijkstraqueue = new myNodeDijkstraPriorityQueue(property_type);
        myDijNode snode = new myDijNode(sid, 0, null);
        dijkstraqueue.add(snode);
        nodelist.put(snode.node_id, snode);
        snode.inqueue = true;

        try (Transaction tx = this.neo4j.graphDB.beginTx()) {

            // int i =0;
            while (!dijkstraqueue.isEmpty()) {
                myDijNode n = dijkstraqueue.pop();
//                System.out.println(n.node_id + " " + n.dist + " " + n.prev);
                ArrayList<Relationship> rels = n.getNeighbor(property_type, this.neo4j);

                if (n.node_id == did) {
                    break;
                }

                for (Relationship rel : rels) {
                    myDijNode next_node = nodelist.get(rel.getOtherNodeId(n.node_id));

                    double old_cost = nodelist.get(n.node_id).dist;
                    double rel_cost = (double) rel.getProperty(property_type);
                    double new_cost = old_cost + rel_cost;

                    if (next_node == null) {
                        next_node = new myDijNode(rel.getOtherNodeId(n.node_id), new_cost, n);
                        nodelist.put(next_node.node_id, next_node);
                        if (next_node.inqueue == false) {
                            dijkstraqueue.add(next_node);
                        }
                    } else if (next_node.dist > new_cost) {
                        next_node.dist = new_cost;
                        next_node.prev = n;
                        nodelist.put(next_node.node_id, next_node);
                        if (next_node.inqueue == false) {
                            dijkstraqueue.add(next_node);
                        }
                    }


                }
            }

            tx.success();
        }
        return nodelist.get(did).dist;
    }


    private <T> T getRandomNodes(ArrayList<T> nodelist) {
        Random r = new Random();
        int idx = r.nextInt(nodelist.size());
        return nodelist.get(idx);
    }

    public void clear() {
        this.results.clear();
        this.monitor.clear();
    }


}

class myDijNode {
    long node_id;
    double dist;
    myDijNode prev;
    boolean inqueue = false;

    public myDijNode(long node_id, double dist, myDijNode prev) {
        this.node_id = node_id;
        this.dist = dist;
        this.prev = prev;
    }

    public ArrayList<Relationship> getNeighbor(String property_type, Neo4jDB neo4j) {
        ArrayList<Relationship> rels = new ArrayList<>();
        try (Transaction tx = neo4j.graphDB.beginTx()) {
            Iterator<Relationship> rels_iter = neo4j.graphDB.getNodeById(node_id).getRelationships(Direction.BOTH).iterator();
            while (rels_iter.hasNext()) {
                rels.add(rels_iter.next());
            }
            tx.success();
        }
        return rels;
    }
}

class myNodeDijkstraPriorityQueue {
    PriorityQueue<myDijNode> queue;
    String propertiy_type;

    public myNodeDijkstraPriorityQueue(String propertiy_type) {
        myDijComparator mc = new myDijComparator(propertiy_type);
        this.queue = new PriorityQueue<myDijNode>(1000000, mc);
        this.propertiy_type = propertiy_type;
    }

    public boolean add(myDijNode p) {
        return this.queue.add(p);
    }

    public int size() {
        return this.queue.size();
    }

    public boolean isEmpty() {
        return this.queue.isEmpty();
    }

    public myDijNode pop() {
        return this.queue.poll();
    }

}

class myDijComparator implements Comparator<myDijNode> {
    String propertiy_type;

    public myDijComparator(String propertiy_type) {
        super();
        this.propertiy_type = propertiy_type;

    }

    public int compare(myDijNode x, myDijNode y) {
        if (x.dist - y.dist == 0) {
            return 0;
        } else if (x.dist - y.dist < 0) {
            return -1;
        } else {
            return 1;
        }
    }
}
