package Query.landmark;

import Neo4jTools.Line;
import Neo4jTools.Neo4jDB;
import Query.IndexFlat;
import Query.backbonePath;
import Query.tools.SortByNodeId;
import Query.tools.myFileFilter;
import org.apache.commons.io.FileUtils;
import org.neo4j.cypher.internal.v3_4.functions.Rand;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.api.impl.fulltext.analyzer.providers.Arabic;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class LandmarkBBS {


    private GraphDatabaseService graphdb;
    public Neo4jDB neo4j;
    public HashSet<Long> node_list = new HashSet<>();
    public IndexFlat flatindex;

    /**
     * landmark nodes --> <dest nodes, <the value of shortest path from landmark nodes to dest nodes in each dimension>>
     **/
    public HashMap<Long, HashMap<Long, double[]>> landmark_index = new HashMap<>();

    public LandmarkBBS(String db_name, IndexFlat flatindex) {
        String sub_db_name = db_name;
        neo4j = new Neo4jDB(sub_db_name);
        neo4j.startDB(true);
        graphdb = neo4j.graphDB;
        System.out.println(neo4j.DB_PATH + "  number of nodes:" + neo4j.getNumberofNodes() + "   number of edges : " + neo4j.getNumberofEdges());
        this.node_list = this.neo4j.getNodes();
        this.flatindex = flatindex;
    }


    public LandmarkBBS(String db_name) {
        String sub_db_name = db_name;
        neo4j = new Neo4jDB(sub_db_name);
        neo4j.startDB(true);
        graphdb = neo4j.graphDB;
        System.out.println(neo4j.DB_PATH + "  number of nodes:" + neo4j.getNumberofNodes() + "   number of edges : " + neo4j.getNumberofEdges());
        this.node_list = this.neo4j.getNodes();
    }

    public LandmarkBBS(String sub_db_name, HashMap<Long, HashMap<Long, double[]>> landmark_index) {
        neo4j = new Neo4jDB(sub_db_name);
        neo4j.startDB(true);
        graphdb = neo4j.graphDB;
        System.out.println(neo4j.DB_PATH + "  number of nodes:" + neo4j.getNumberofNodes() + "   number of edges : " + neo4j.getNumberofEdges());
        this.node_list = this.neo4j.getNodes();

        this.landmark_index = new HashMap<>(landmark_index);
    }

    public void buildLandmarkIndex(int num_landmarks, ArrayList<Long> landmark_list_ids) {
        this.landmark_index.clear();

        try (Transaction tx = this.neo4j.graphDB.beginTx()) {

            ArrayList<Node> nodelist = new ArrayList<>();

            ResourceIterable<Node> nodes_iterable = this.neo4j.graphDB.getAllNodes();
            ResourceIterator<Node> nodes_iter = nodes_iterable.iterator();
            while (nodes_iter.hasNext()) {
                Node node = nodes_iter.next();
                nodelist.add(node);
            }

            ArrayList<Node> landmarks = getLandMarkNodeList(num_landmarks, landmark_list_ids, nodelist);

            for (Node lnode : landmarks) {
                HashMap<Long, double[]> index_from_landmark_to_dest = new HashMap<>();
                System.out.println("Build the index for the node " + lnode);
                int index = 0;

//                for (String property_name : Neo4jDB.propertiesName) {
//                    System.out.println("Attribute :  " + property_name);
//                }

                for (Node destination : nodelist) {
                    if ((++index) % 500 == 0) {
                        System.out.println(lnode + "    " + index + " ..............................");
                    }

                    int i = 0;
                    double[] min_costs = new double[3];
                    for (String property_name : Neo4jDB.propertiesName) {
                        PathFinder<WeightedPath> finder = GraphAlgoFactory.dijkstra(PathExpanders.forTypeAndDirection(Line.Linked, Direction.BOTH), property_name);
                        WeightedPath paths = finder.findSinglePath(lnode, destination);
                        if (paths != null) {
                            min_costs[i] = paths.weight();
                            i++;
                        } else {
                            System.out.println("Can not find a shortest path from " + lnode + " to " + destination);
                            System.exit(0);
                        }
                    }

                    index_from_landmark_to_dest.put(destination.getId(), min_costs);
                }

                this.landmark_index.put(lnode.getId(), index_from_landmark_to_dest);
            }

            tx.success();
        }
    }

    public void readLandmarkIndex(int num_landmarks, ArrayList<Long> landmark_list_ids, boolean createNew) {

        if (landmark_list_ids.size() > num_landmarks) {
            num_landmarks = landmark_list_ids.size();
        }

        String landmark_index_folder_base = "/home/gqxwolf/mydata/projectData/BackBone/indexes/landmarks/";

        String landmark_index_folder = landmark_index_folder_base + this.neo4j.dbname;
        File landmark_index_fl = new File(landmark_index_folder);
        System.out.println("Read the landmark index from the foler ====>> " + landmark_index_folder);

        boolean null_landmark_list = false;
        if (landmark_list_ids == null || landmark_list_ids.size() == 0) {
            null_landmark_list = true;
        }

        try {
            if (!landmark_index_fl.exists()) {
                FileUtils.forceMkdir(landmark_index_fl);
            }

            File[] idx_files = landmark_index_fl.listFiles(new myFileFilter());
            HashMap<Long, Boolean> readed_idx_file = new HashMap<>();
            HashMap<Long, File> lamdmark_idx_file_mapping = new HashMap<>();

            for (File idx_f : idx_files) {
                long landmark = Long.parseLong(idx_f.getName().substring(0, idx_f.getName().lastIndexOf(".")));
                //Read the specific landmark index file if the landmark nodes are given by landmark_list_ids
                if (!null_landmark_list && landmark_list_ids.contains(landmark)) {
                    BufferedReader b = new BufferedReader(new FileReader(idx_f));
                    String readLine = "";
                    HashMap<Long, double[]> landmark_contents = new HashMap<>();
                    while ((readLine = b.readLine()) != null) {
                        String[] infos = readLine.split(" ");
                        double[] costs = new double[infos.length - 1];

                        long target_node = Long.parseLong(infos[0]);
                        costs[0] = Double.parseDouble(infos[1]);
                        costs[1] = Double.parseDouble(infos[2]);
                        costs[2] = Double.parseDouble(infos[3]);

                        landmark_contents.put(target_node, costs);
                    }

                    this.landmark_index.put(landmark, landmark_contents);
                    readed_idx_file.put(landmark, true);
                    System.out.println("Finished the reading of landmark index for node [" + landmark + "]");

                } else {
                    readed_idx_file.put(landmark, false);
                }
                lamdmark_idx_file_mapping.put(landmark, idx_f);
            }

            //random read the landmark index
            if (!createNew && idx_files.length >= num_landmarks) {

                if (!null_landmark_list) {
                    ArrayList<Long> remained_landmarks = landmark_list_ids.stream().filter(node_id -> !landmark_index.containsKey(node_id)).collect(Collectors.toCollection(ArrayList::new));
                    buildLandmarkIndex(remained_landmarks);
                    for (long landmark : remained_landmarks) {
                        HashMap<Long, double[]> mapping = this.landmark_index.get(landmark);
                        String idx_file_name = landmark_index_folder + "/" + landmark + ".idx";
                        writeLandmarkIndexToDisk(idx_file_name, mapping);
                        System.out.println("Finished the writing index to disk [" + landmark + "]    ");
                    }
                }

                System.out.println("Begin to read the landmark index ........................................");
                while (this.landmark_index.size() < num_landmarks) {
                    Random r = new Random(System.currentTimeMillis());

                    //Keep the unread landmark node list, that is used to random pick
                    ArrayList<Long> unreaded_landmark = new ArrayList<>();
                    for (long landmark : readed_idx_file.keySet()) {
                        if (!readed_idx_file.get(landmark)) {
                            unreaded_landmark.add(landmark);
                        }
                    }

                    long landmark = unreaded_landmark.get(r.nextInt(unreaded_landmark.size()));
                    readed_idx_file.put(landmark, true); //mark this landmark is read. Don't read it again.
                    File ldm_file = lamdmark_idx_file_mapping.get(landmark);

                    BufferedReader b = new BufferedReader(new FileReader(ldm_file));
                    String readLine = "";
                    HashMap<Long, double[]> landmark_contents = new HashMap<>();
                    while ((readLine = b.readLine()) != null) {
                        String[] infos = readLine.split(" ");
                        double[] costs = new double[infos.length - 1];

                        long target_node = Long.parseLong(infos[0]);
                        costs[0] = Double.parseDouble(infos[1]);
                        costs[1] = Double.parseDouble(infos[2]);
                        costs[2] = Double.parseDouble(infos[3]);

                        landmark_contents.put(target_node, costs);
                    }

                    this.landmark_index.put(landmark, landmark_contents);
                    System.out.println("Finished the reading of landmark index for node [" + landmark + "]");
                }
                System.out.println("End the process of reading the landmark index ........................................");
            } else { //create the new landmark nodes
                buildLandmarkIndex(num_landmarks, landmark_list_ids);
                for (long landmark : this.landmark_index.keySet()) {
                    HashMap<Long, double[]> mapping = landmark_index.get(landmark);
                    String idx_file_name = landmark_index_folder + "/" + landmark + ".idx";
                    writeLandmarkIndexToDisk(idx_file_name, mapping);
                    System.out.println("Finished the writing index to disk [" + landmark + "]    ");
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void buildLandmarkIndex(ArrayList<Long> landmark_list_ids) {
        try (Transaction tx = this.neo4j.graphDB.beginTx()) {

            ArrayList<Node> nodelist = new ArrayList<>();

            ResourceIterable<Node> nodes_iterable = this.neo4j.graphDB.getAllNodes();
            ResourceIterator<Node> nodes_iter = nodes_iterable.iterator();
            while (nodes_iter.hasNext()) {
                Node node = nodes_iter.next();
                nodelist.add(node);
            }

            ArrayList<Node> landmarks = getLandMarkNodeList(landmark_list_ids, nodelist);

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
                        PathFinder<WeightedPath> finder = GraphAlgoFactory.dijkstra(PathExpanders.forTypeAndDirection(Line.Linked, Direction.BOTH), property_name);
                        WeightedPath paths = finder.findSinglePath(lnode, destination);
                        if (paths != null) {
                            min_costs[i] = paths.weight();
                            i++;
                        } else {
                            System.out.println("Can not find a shortest path from " + lnode + " to " + destination);
                            System.exit(0);
                        }
                    }

                    index_from_landmark_to_dest.put(destination.getId(), min_costs);
                }

                this.landmark_index.put(lnode.getId(), index_from_landmark_to_dest);
            }

            tx.success();
        }
    }

    private void writeLandmarkIndexToDisk(String idx_file_name, HashMap<Long, double[]> mapping) {
        FileWriter fileWriter = null;
        try {

            if (new File(idx_file_name).exists()) {
                new File(idx_file_name).delete();
            }

            fileWriter = new FileWriter(idx_file_name);
            PrintWriter printWriter = new PrintWriter(fileWriter);
            for (Map.Entry<Long, double[]> landmark_element : mapping.entrySet()) {
                long dest_node_id = landmark_element.getKey();
                double[] costs = landmark_element.getValue();
                printWriter.println(dest_node_id + " " + costs[0] + " " + costs[1] + " " + costs[2]);

            }
            printWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private ArrayList<Node> getLandMarkNodeList(int num_landmarks, ArrayList<Long> landmark_list_ids, ArrayList<Node> nodelist) {
        ArrayList<Node> result_list = new ArrayList<>();

        HashMap<Long, Node> node_mapping = new HashMap<>();
        for (Node n : nodelist) {
            node_mapping.put(n.getId(), n);
        }

        if (landmark_list_ids == null || landmark_list_ids.size() == 0) {
            while (result_list.size() < num_landmarks) {
                Node landmarks_node = getRandomNodes(nodelist);
                if (!result_list.contains(landmarks_node)) {
                    result_list.add(landmarks_node);
                }
            }
        } else {
            for (long ldm_id : landmark_list_ids) {
                result_list.add(node_mapping.get(ldm_id));
            }
        }

        return result_list;
    }

    private ArrayList<Node> getLandMarkNodeList(ArrayList<Long> landmark_list_ids, ArrayList<Node> nodelist) {
        ArrayList<Node> result_list = new ArrayList<>();

        HashMap<Long, Node> node_mapping = new HashMap<>();
        for (Node n : nodelist) {
            node_mapping.put(n.getId(), n);
        }

        if (landmark_list_ids != null && landmark_list_ids.size() != 0) {
            for (long landmarks_node_id : landmark_list_ids) {
                Node landmarks_node = node_mapping.get(landmarks_node_id);
                if (!result_list.contains(landmarks_node)) {
                    result_list.add(landmarks_node);
                }
            }
        }

        return result_list;
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

    public void landmark_bbs(long source_node, long dest_node, HashMap<Long, ArrayList<backbonePath>> source_nodes_list, HashMap<Long, ArrayList<backbonePath>> all_possible_dest_node_with_skypaths, ArrayList<backbonePath> results) {
        HashMap<Long, myNode> tmpStoreNodes = new HashMap();

        try (Transaction tx = this.graphdb.beginTx()) {
            myNodePriorityQueue mqueue = new myNodePriorityQueue();
            for (Map.Entry<Long, ArrayList<backbonePath>> source_e : source_nodes_list.entrySet()) {
                myNode snode = new myNode(source_node, dest_node, source_e, all_possible_dest_node_with_skypaths, neo4j);
                mqueue.add(snode);
                tmpStoreNodes.put(snode.id, snode);
                System.out.println("Add the source node to the queue ::::::>>>>>>>   " + snode.id + "   ");
            }
            System.out.println("[Init] there are " + mqueue.size() + "  in the queue at the beginning !!!!!!!!");


            while (!mqueue.isEmpty()) {
                myNode v = mqueue.pop();
                v.inqueue = false;

                for (int i = 0; i < v.skyPaths.size(); i++) {
                    backbonePath p = v.skyPaths.get(i);

                    if (!p.p.expanded) {
                        p.p.expanded = true;

                        if (landmark_index.size() != 0) {
                            updateThePathDestinationList(p, results);
                        }

                        //Still can be expand to any of the destination
                        if (!p.p.possible_destination.isEmpty()) {

                            ArrayList<backbonePath> new_paths = p.expand(neo4j);
//                            ArrayList<backbonePath> flat_new_paths = flatindex.expand(p);
//                            new_paths.addAll(flat_new_paths);

                            for (backbonePath new_bp : new_paths) {

                                if (new_bp.hasCycle || dominatedByResult(new_bp.costs, results)) {
                                    continue;
                                }

                                myNode next_n;
                                if (tmpStoreNodes.containsKey(new_bp.destination)) {
                                    next_n = tmpStoreNodes.get(new_bp.destination);
                                } else {
                                    next_n = new myNode(source_node, dest_node, new_bp.destination, all_possible_dest_node_with_skypaths, neo4j);
                                    tmpStoreNodes.put(next_n.id, next_n);
                                }

                                if (new_bp.p.possible_destination.containsKey(next_n.id)) {
                                    for (backbonePath d_skyline_bp : new_bp.p.possible_destination.get(next_n.id)) {
                                        backbonePath final_bp = new backbonePath(new_bp, d_skyline_bp, true);
                                        addToSkyline(results, final_bp);
                                    }

                                    new_bp.p.possible_destination.remove(next_n.id);

                                    if (new_bp.p.possible_destination.size() != 0) {
                                        if (next_n.addToSkyline(new_bp) && !next_n.inqueue) {
                                            mqueue.add(next_n);
                                        }
                                    }
                                } else if (next_n.addToSkyline(new_bp) && !next_n.inqueue) {
                                    mqueue.add(next_n);
                                }
                            }
                        }
                    }
                }
            }
            tx.success();
        }
    }


    private void updateThePathDestinationList(backbonePath p, ArrayList<backbonePath> results) {
        ArrayList<Long> deleted_dest_nodes = new ArrayList<>();

        for (long dest_highway_node_id : p.p.possible_destination.keySet()) {

            ArrayList<backbonePath> dest_skyline = p.p.possible_destination.get(dest_highway_node_id);

            //Todo: could be optimized by using the max or min values of the all the skyline paths from dest_highway to dest, so, it will not iterate all the skyline paths
            for (int dp_idx = 0; dp_idx < dest_skyline.size(); ) {
                backbonePath dest_skyline_bp = dest_skyline.get(dp_idx); //the skyline paths from dest_highways to destination node
                double[] p_l_costs = getLowerBound(p.costs, p.destination, dest_skyline_bp);
                if (dominatedByResult(p_l_costs, results)) {
                    dest_skyline.remove(dp_idx);//remove if dominate
                } else {
                    dp_idx++;
                }
            }

            //all of the skyline paths from dest_highway nodes to dest nodes can not be a candidate results from this path p to destination node
            if (dest_skyline.size() == 0) {
                deleted_dest_nodes.add(dest_highway_node_id);
            }
        }

        for (long deleted_node : deleted_dest_nodes) {
            p.p.possible_destination.remove(deleted_node);
        }
    }


    public ArrayList<backbonePath> initResultShortestPath
            (Map.Entry<Long, ArrayList<backbonePath>> src_set, HashMap<Long, ArrayList<backbonePath>> dest_set) {
        ArrayList<backbonePath> init_result = new ArrayList<>();
        long src = src_set.getKey();

        for (Map.Entry<Long, ArrayList<backbonePath>> dest_element : dest_set.entrySet()) {
            backbonePath bp;
            long dest = dest_element.getKey();
            try (Transaction tx = this.neo4j.graphDB.beginTx()) {
                Node destination = this.neo4j.graphDB.getNodeById(dest);
                Node startNode = this.neo4j.graphDB.getNodeById(src);

                for (String property_name : Neo4jDB.propertiesName) {
                    PathFinder<WeightedPath> finder = GraphAlgoFactory
                            .dijkstra(PathExpanders.forTypeAndDirection(Line.Linked, Direction.BOTH), property_name);
                    WeightedPath paths = finder.findSinglePath(startNode, destination);
                    if (paths != null) {
                        bp = new backbonePath(paths, this.neo4j);
                        for (backbonePath src_to_bp : src_set.getValue()) {
                            for (backbonePath to_dest_dp : dest_set.get(dest)) {
                                backbonePath last_part_dp = new backbonePath(bp, to_dest_dp, false);
                                backbonePath init_dp = new backbonePath(src_to_bp, last_part_dp, false);
                                addToSkyline(init_result, init_dp);
                            }
                        }
                    }
                }
                tx.success();
            }
        }
        return init_result;
    }


    public ArrayList<backbonePath> initResultLandMark
            (Map.Entry<Long, ArrayList<backbonePath>> src_set, HashMap<Long, ArrayList<backbonePath>> dest_set) {
        ArrayList<backbonePath> init_result = new ArrayList<>();
        long src = src_set.getKey();

        for (long dest : dest_set.keySet()) {
            double[] costs_upperbound = new double[3];
            for (int i = 0; i < costs_upperbound.length; i++) {
                costs_upperbound[i] = Double.POSITIVE_INFINITY;
            }

            for (long landmark : this.landmark_index.keySet()) {
                double[] src_cost = this.landmark_index.get(landmark).get(src); //the source node (the destination of the current path) to landmark
                double[] dest_cost = this.landmark_index.get(landmark).get(dest); // the dest_highways to landmark
                for (int i = 0; i < costs_upperbound.length; i++) {
                    double value = Math.abs(src_cost[i] + dest_cost[i]);
                    if (value < costs_upperbound[i]) {
                        costs_upperbound[i] = value;
                    }
                }

                System.out.println(landmark+"    "+costs_upperbound[0] + " " + costs_upperbound[1] + " " + costs_upperbound[2]);

            }


            for (backbonePath src_to_bp : src_set.getValue()) {
                System.out.println(src_to_bp);
                for (backbonePath dest_dp : dest_set.get(dest)) {
                    System.out.println("            " + dest_dp);
                    double[] to_dest_cost = new double[3];

                    for (int i = 0; i < to_dest_cost.length; i++) {
                        to_dest_cost[i] = costs_upperbound[i] + dest_dp.costs[i];
                    }

                    backbonePath upper_bp = new backbonePath(src, dest, to_dest_cost);
                    backbonePath init_bp = new backbonePath(src_to_bp, upper_bp, false);

                    addToSkyline(init_result, init_bp);
                }
            }
        }

        return init_result;
    }


    public boolean addToSkyline(ArrayList<backbonePath> bp_list, backbonePath bp) {
        int i = 0;

        if (bp_list.isEmpty()) {
            bp_list.add(bp);
            return true;
        } else {
            boolean can_insert_np = true;
            for (; i < bp_list.size(); ) {
                if (checkDominated(bp_list.get(i).costs, bp.costs)) {
                    can_insert_np = false;
                    break;
                } else {
                    if (checkDominated(bp.costs, bp_list.get(i).costs)) {
                        bp_list.remove(i);
                    } else {
                        i++;
                    }
                }
            }
            if (can_insert_np) {
                bp_list.add(bp);
                return true;
            }
        }
        return false;
    }

    private double[] getLowerBound(double[] costs, long src, backbonePath dest_dp) {
        double[] estimated_costs = new double[3];

        for (int i = 0; i < estimated_costs.length; i++) {
            estimated_costs[i] = Double.NEGATIVE_INFINITY;
        }

        for (long landmark : this.landmark_index.keySet()) {
            double[] src_cost = this.landmark_index.get(landmark).get(src); //the source node (the destination of the current path) to landmark
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
            if (costs[i] > estimatedCosts[i]) {
                return false;
            }
        }
        return true;
//        int numberNotEqual = 0;
//        for (int i = 0; i < costs.length; i++) {
//            if (costs[i] > estimatedCosts[i]) {
//                return false;
//            } else if (costs[i] < estimatedCosts[i] && numberNotEqual == 0) {
//                numberNotEqual++;
//            }
//        }
//
//        if (numberNotEqual != 0) {
//            return true;
//        } else {
//            return false;
//        }
    }
}
