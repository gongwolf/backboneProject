package Query.landmark;

import Neo4jTools.Line;
import Neo4jTools.Neo4jDB;
import Query.backbonePath;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.*;

import java.util.*;

public class LandmarkBBS {


    private GraphDatabaseService graphdb;
    public Neo4jDB neo4j;
    public HashSet<Long> node_list = new HashSet<>();

    /**
     * landmark nodes --> <dest nodes, <the value of shortest path from landmark nodes to dest nodes in each dimension>>
     **/
    public HashMap<Long, HashMap<Long, double[]>> landmark_index = new HashMap<>();


    public LandmarkBBS(String db_name) {
        String sub_db_name = db_name;
        neo4j = new Neo4jDB(sub_db_name);
        neo4j.startDB(true);
        graphdb = neo4j.graphDB;
        System.out.println(neo4j.DB_PATH + "  number of nodes:" + neo4j.getNumberofNodes() + "   number of edges : " + neo4j.getNumberofEdges());
        this.node_list = this.neo4j.getNodes();
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
            this.neo4j.closeDB();
        }
    }

    private <T> T getRandomNodes(ArrayList<T> nodelist) {
        Random r = new Random();
        int idx = r.nextInt(nodelist.size());
        return nodelist.get(idx);
    }

    public void landmark_bbs(long source_node, Map.Entry<Long, ArrayList<backbonePath>> source_skyline_paths_costs, HashMap<Long, ArrayList<backbonePath>> destination_highways_results) {
        HashMap<Long, myNode> tmpStoreNodes = new HashMap();
        ArrayList<backbonePath> results = new ArrayList<>();

        HashMap<Long, ArrayList<backbonePath>> all_possible_dest_node_with_skypaths = new HashMap<>();
        for (Map.Entry<Long, ArrayList<backbonePath>> e : destination_highways_results.entrySet()) {
            if (this.node_list.contains(e.getKey())) {
                all_possible_dest_node_with_skypaths.put(e.getKey(), e.getValue());
            }
        }


        try (Transaction tx = this.graphdb.beginTx()) {

            myNode snode = new myNode(source_node, source_skyline_paths_costs, all_possible_dest_node_with_skypaths, neo4j);
            myNodePriorityQueue mqueue = new myNodePriorityQueue();
            mqueue.add(snode);
            tmpStoreNodes.put(snode.id, snode);
            snode.inqueue = true;

            while (!mqueue.isEmpty()) {
                myNode v = mqueue.pop();
                v.inqueue = false;

                for (int i = 0; i < v.skyPaths.size(); i++) {
                    backbonePath p = v.skyPaths.get(i);

                    if (!p.expaned) {

                        if (landmark_index.size() != 0) {
                            updateThePathDestinationList(p, results);
                        }

                        //Still can be expand to any of the destination
                        if (!p.possible_destination.isEmpty()) {
                            ArrayList<path> new_paths = p.expand(neo4j);
                        }
                    }
                }
            }


            tx.success();
        }
    }

    private void updateThePathDestinationList(path p, ArrayList<backbonePath> results) {
        ArrayList<Long> deleted_dest_nodes = new ArrayList<>();

        for (Map.Entry<Long, ArrayList<backbonePath>> dest_element : p.possible_destination.entrySet()) {
            long dest_highway = dest_element.getKey();

            ArrayList<backbonePath> dest_skyline = dest_element.getValue();

            for (int dp_idx = 0; dp_idx < dest_skyline.size(); ) {
                backbonePath dest_skyline_bp = dest_skyline.get(dp_idx); //the skyline paths from dest_highways to destination node
                double[] p_l_costs = getLowerBound(p.costs, p.endNode, dest_skyline_bp);
                if (dominatedByResult(p_l_costs, results)) {
                    dest_skyline.remove(dp_idx);//remove if dominate
                } else {
                    dp_idx++;
                }
            }

            //all of the skyline paths from dest_highway nodes to dest nodes can not be a candidate results from this path p to destination node
            if (dest_skyline.size() == 0) {
                deleted_dest_nodes.add(dest_highway);
            }
        }

        for (long deleted_node : deleted_dest_nodes) {
            p.possible_destination.remove(deleted_node);
        }
    }

    private double[] getLowerBound(double[] costs, long src, backbonePath dest_dp) {
        double[] estimated_costs = new double[3];

        for (int i = 0; i < estimated_costs.length; i++) {
            estimated_costs[i] = Double.NEGATIVE_INFINITY;
        }

        for (long landmark : this.landmark_index.keySet()) {
            double[] src_cost = this.landmark_index.get(landmark).get(src);
            double[] dest_cost = this.landmark_index.get(landmark).get(dest_dp.destination); // the dest_highways to landmark
            for (int i = 0; i < estimated_costs.length; i++) {
                double value = Math.abs(src_cost[i] - dest_cost[i]);
                if (value > estimated_costs[i]) {
                    estimated_costs[i] = value;
                }
            }
        }

        for (int i = 0; i < estimated_costs.length; i++) {
            estimated_costs[i] += costs[i] + dest_dp.costs[i];
        }
        return estimated_costs;
    }

    private boolean dominatedByResult(double[] estimated_costs, ArrayList<backbonePath> results) {
        for (backbonePath rp : results) {
            if (checkDominated(rp.costs, estimated_costs)) {
                return true;
            }
        }

        return false;
    }

    private boolean checkDominated(double[] costs, double[] estimatedCosts) {
        for (int i = 0; i < costs.length; i++) {
            if (costs[i] * (1) > estimatedCosts[i]) {
                return false;
            }
        }
        return true;
    }

}
