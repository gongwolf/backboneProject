package Query;

import Baseline.path;
import DataStructure.Monitor;
import Query.Queue.MyQueue;
import Query.Queue.myBackNode;
import Query.landmark.LandmarkBBS;

import java.io.*;
import java.util.*;

public class QueryProcess {
    private final int index_level;
    private final Monitor monitor;


    String system_home_folder = System.getProperty("user.home");
    String str_number_of_nodes = "50K";
//    String source_file_folder = "busline_sub_graph_NY_" + str_number_of_nodes;
//    String index_files_folder = system_home_folder + "/mydata/projectData/BackBone/indexes/" + source_file_folder;
    String source_file_folder = "busline_sub_graph_NY";
    String index_files_folder = system_home_folder + "/mydata/projectData/BackBone/indexes/busline_sub_graph_NY";
    BackBoneIndex index;
    private long surfix = 0;

    HashMap<Long, ArrayList<backbonePath>> source_to_highway_results = new HashMap<>(); //the temporary results from source node to highways
    HashMap<Long, ArrayList<backbonePath>> destination_to_highway_results = new HashMap<>(); //the temporary results from destination node to highways

    public ArrayList<backbonePath> result = new ArrayList<>();
    public LandmarkBBS bbs;

    public QueryProcess() {
        index = new BackBoneIndex(index_files_folder);
        this.index_level = index.total_level;
        String sub_db_name = "sub_ny_USA_Level" + this.index_level;
//        String sub_db_name = "sub_ny_USA_" + this.str_number_of_nodes + "_Level" + this.index_level;
        bbs = new LandmarkBBS(sub_db_name, index.flatindex);

        index.buildFlatIndex();
//        index.buildHighestFlatIndex(bbs.node_list, true);

        ArrayList<Long> ldms = new ArrayList<>();
//        ldms.add(131l);
//        ldms.add(9165l);
//        ldms.add(2540l);
//        ldms.add(455l);
//        ldms.add(4559l);
//        ldms.add(5901l);

//
//        bbs.buildLandmarkIndex(3, ldms);

        ldms.add(5699l);
        ldms.add(4069l);
        ldms.add(270l);
        bbs.readLandmarkIndex(3, ldms, false);

        this.monitor = new Monitor();

        Date date = new Date();
        this.surfix = date.getTime();

    }

    public static void main(String args[]) {

        ArrayList<Integer> resultset_number = new ArrayList<>();
        for (int i = 0; i < 1; i++) {
            QueryProcess query = new QueryProcess();
            long running_time = System.nanoTime();
            query.query(3227, 8222, false);
            System.out.println("Total Runningt time is " + (System.nanoTime() - running_time) / 1000000 + " ms ");
            resultset_number.add(query.result.size());
            query.bbs.closeDB();
        }

        System.out.println("===========================================================================================");
        for (int size : resultset_number) {
            System.out.print(size + "  ");
        }

    }

    public void flat_query(long source_node, long destination_node, boolean result_in_each_layer) {
        System.out.println("=================================================================================");
        findCommonLayer(source_node, destination_node);
        System.out.println("=================================================================================");

        System.out.println("~~~~~~~~~~~~~~~~~~~~~ find the result in the layer index structure ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        findTheIndexInTheLayerIndex(source_node, destination_node, result_in_each_layer);

        System.out.println("~~~~~~~~~~~~~~~~~~~~~ find the common highway nodes at lower level (except the highest level) ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        HashSet<Long> commonset = findCommandHighways(source_to_highway_results.keySet(), destination_to_highway_results.keySet());
        if (!commonset.isEmpty()) {
            for (long common_node : commonset) {
                combinationResult(source_to_highway_results.get(common_node), destination_to_highway_results.get(common_node));
            }
        }
        printResult(result);

        System.out.println("~~~~~~~~~~~~~~~~~~~~~ find the highway nodes at the highest level                      ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

        findAtTheHighestLevelFlat(source_node, destination_node);
        String path_name = "/home/gqxwolf/mydata/projectData/BackBone/" + source_file_folder + "/results";
        saveToDisk(path_name + "/backbone_3227_8222_landmark_init_flat.txt");
    }

    private void findTheIndexInTheLayerIndex(long source_node, long destination_node, boolean result_in_each_layer) {

        source_to_highway_results.clear();
        destination_to_highway_results.clear();

        backbonePath sourceDummyResult = new backbonePath(source_node);
        ArrayList<backbonePath> temp_src_list = new ArrayList<>();
        temp_src_list.add(sourceDummyResult);
        source_to_highway_results.put(source_node, temp_src_list);

        //the destination to it self
        backbonePath destDummyResult = new backbonePath(destination_node);
        ArrayList<backbonePath> temp_dest_list = new ArrayList<>();
        temp_dest_list.add(destDummyResult);
        destination_to_highway_results.put(destination_node, temp_dest_list);

//        HashSet<Long> needs_to_add_to_source = new HashSet<>();
//        HashSet<Long> needs_to_add_to_destination = new HashSet<>();


        for (int l = 0; l < this.index_level; l++) {
            System.out.println("Find the index information at level " + l);
            System.out.println("==================================================================================");

            if (result_in_each_layer) {
                ArrayList<backbonePath> temp_result = new ArrayList<>();
                HashMap<Long, ArrayList<backbonePath>> all_possible_dest_node_with_skypaths = filterPossibleNodeInList(destination_to_highway_results);
                HashMap<Long, ArrayList<backbonePath>> source_list = filterPossibleNodeInList(source_to_highway_results);
                String sub_db_name = "sub_ny_USA_Level" + l;
                LandmarkBBS layer_bbs = new LandmarkBBS(sub_db_name);
                System.out.println("BBS query on the graph " + layer_bbs.neo4j.graphDB);
                System.out.println("source list : " + source_list.keySet());
                System.out.println("destination list : " + all_possible_dest_node_with_skypaths.keySet());
                layer_bbs.buildLandmarkIndex(3, null);

                ArrayList<Long> sorted_source_list = new ArrayList<>(source_list.keySet());
                Random random = new Random();
                int random_source_idx = random.nextInt(sorted_source_list.size());
                Set<Map.Entry<Long, ArrayList<backbonePath>>> entrySet = source_list.entrySet();
                ArrayList<Map.Entry<Long, ArrayList<backbonePath>>> listOfSources = new ArrayList<>(entrySet);
                System.out.println("Choose the index with " + random_source_idx + "  ====>>>>  " + listOfSources.get(random_source_idx).getKey());
                ArrayList<backbonePath> layer_init_paths = layer_bbs.initResultLandMark(listOfSources.get(random_source_idx), all_possible_dest_node_with_skypaths);
                for (backbonePath init_bp : layer_init_paths) {
                    addToSkyline(temp_result, init_bp);
                }

                long rt = System.currentTimeMillis();
                layer_bbs.landmark_bbs(source_node, destination_node, source_list, all_possible_dest_node_with_skypaths, temp_result);
                String path_name = "/home/gqxwolf/mydata/projectData/BackBone/" + source_file_folder + "/results";
                saveToDisk(temp_result, path_name + "/backbone_" + source_node + "_" + destination_node + "_query_on_level" + l +"_"+this.surfix+".txt");
                layer_bbs.closeDB();
                System.out.println("The BBS query at level " + l + " found " + temp_result.size() + "  skyline paths  in " + (System.currentTimeMillis() - rt) + "ms");
                System.out.println("==================================================================================");
            }

            HashSet<Long> shList = new HashSet<>(source_to_highway_results.keySet());
            HashSet<Long> dhList = new HashSet<>(destination_to_highway_results.keySet());

            for (long s_id : shList) {
                HashSet<Long> highwaysOfsrcNode = this.index.getHighwayNodeAtLevel(l, s_id); //get highways of s_id
                if (highwaysOfsrcNode != null) {
                    for (long h_node : highwaysOfsrcNode) {//h_node is highway node of the sid, it's the source node to the next level
                        ArrayList<double[]> cost_from_src_to_highway = index.readHighwaysInformation(h_node, l, s_id);

                        if (cost_from_src_to_highway != null || !cost_from_src_to_highway.isEmpty()) {
                            if (h_node == destination_node) {
                                ArrayList<backbonePath> bps_src_to_sid = source_to_highway_results.get(s_id); // the backbone paths from source node to s_id;
                                for (backbonePath old_path : bps_src_to_sid) {
                                    for (double[] costs : cost_from_src_to_highway) {
                                        backbonePath new_bp = new backbonePath(h_node, costs, old_path); //the new path from the sid->old_highway->new_highway
                                        addToSkyline(this.result, new_bp);
                                        System.out.println("find highway is the destination node " + s_id + "   " + h_node + " : " + old_path + " --> " + new_bp);
                                        monitor.finnalCallAddToSkyline++;
                                    }
                                }
                            } else {
//                                boolean needtoinserted = false;
                                ArrayList<backbonePath> bps_src_to_sid = source_to_highway_results.get(s_id);
                                for (backbonePath old_path : bps_src_to_sid) {
                                    for (double[] costs : cost_from_src_to_highway) {
                                        long s_creat_rt_src = System.nanoTime();
                                        backbonePath new_bp = new backbonePath(h_node, costs, old_path); //the new path from the sid->old_highway->new_highway
//                                        System.out.println(s_id + "   " + h_node + " : " + old_path + " =====================> " + new_bp + " --> " + new_bp.hasCycle);
                                        long e_creat_rt_src = System.nanoTime();
                                        monitor.runningtime_src_create_newpath += (e_creat_rt_src - s_creat_rt_src);

                                        if (!dominatedByResult(new_bp) && !new_bp.hasCycle) {
                                            long s_add_rt_src = System.nanoTime();
                                            boolean flag = addToResultSet(new_bp, source_to_highway_results);
                                            long e_add_rt_src = System.nanoTime();
                                            monitor.runningtime_src_addtoskyline += (e_add_rt_src - s_add_rt_src);
//                                            if (flag && !needtoinserted) {
//                                                needtoinserted = true;
//                                            }
                                        }
                                    }
                                }

//                                if (needtoinserted) {
//                                    needs_to_add_to_source.add(h_node);
//                                }
                            }
                        }
                    }
                }
            }

            System.out.println("~~~~~~~~~~~~~~~~~~~~~~");

            for (long d_id : dhList) {
                HashSet<Long> highwaysOfDestNode = this.index.getHighwayNodeAtLevel(l, d_id);//get highways of did
                if (highwaysOfDestNode != null) {
                    for (long h_node : highwaysOfDestNode) {//h_node is highway node of the did, it's the destination node to the next level
                        ArrayList<double[]> cost_from_dest_to_highway = index.readHighwaysInformation(h_node, l, d_id); //costs from did to h_node

                        if (cost_from_dest_to_highway != null || !cost_from_dest_to_highway.isEmpty()) {
                            if (h_node == source_node) {
                                ArrayList<backbonePath> bps_dest_to_did = destination_to_highway_results.get(d_id); // the backbone paths from destination node to d_id;
                                for (backbonePath old_path : bps_dest_to_did) {
                                    for (double[] costs : cost_from_dest_to_highway) {
                                        backbonePath new_bp = new backbonePath(h_node, costs, old_path); //the new path from the did->old_highway->destination (the highway node)
                                        addToSkyline(this.result, new_bp);
                                        System.out.println("find highway is the source node " + h_node + "     " + d_id + " : " + old_path + " --> " + new_bp);
                                        monitor.finnalCallAddToSkyline++;
                                    }
                                }
                            } else {
//                                boolean needtoinserted = false;
                                ArrayList<backbonePath> bps_dest_to_did = destination_to_highway_results.get(d_id); //the backbone paths from destination to did
                                for (backbonePath old_path : bps_dest_to_did) {
                                    for (double[] costs : cost_from_dest_to_highway) {
                                        long s_creat_rt_dest = System.nanoTime();
                                        backbonePath new_bp = new backbonePath(h_node, costs, old_path); //the new path from the destination->old_highway->new_highway
//                                        System.out.println(h_node + "     " + d_id + " : " + old_path + " =====================> " + new_bp + " --> " + new_bp.hasCycle);
                                        long e_creat_rt_dest = System.nanoTime();
                                        monitor.runningtime_dest_create_newpath += (e_creat_rt_dest - s_creat_rt_dest);
                                        if (!dominatedByResult(new_bp) && !new_bp.hasCycle) {
                                            long s_add_rt_dest = System.nanoTime();
                                            boolean flag = addToResultSet(new_bp, destination_to_highway_results);
                                            long e_add_rt_dest = System.nanoTime();
                                            monitor.runningtime_dest_addtoskyline += (e_add_rt_dest - s_add_rt_dest);

//                                            if (flag && !needtoinserted) {
//                                                needtoinserted = true;
//                                            }
                                        }
                                    }
                                }


//                                if (needtoinserted) {
//                                    needs_to_add_to_destination.add(h_node);
//                                }
                            }
                        }
                    }
                }
            }
            if (this.result.size() != 0) {
                System.out.println("the result at level" + l + ":");
                printResult(result);
            }
            System.out.println("======================================================================");

        }
    }


    private void query(long source_node, long destination_node, boolean result_in_each_layer) {
        System.out.println("=================================================================================");
        findCommonLayer(source_node, destination_node);
        System.out.println("=================================================================================");

        findTheIndexInTheLayerIndex(source_node, destination_node, result_in_each_layer);

        System.out.println("~~~~~~~~~~~~~~~~~~~~~ find the common highway nodes at lower level (except the highest level) ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        HashSet<Long> commonset = findCommandHighways(source_to_highway_results.keySet(), destination_to_highway_results.keySet());
        if (!commonset.isEmpty()) {
            for (long common_node : commonset) {
                combinationResult(source_to_highway_results.get(common_node), destination_to_highway_results.get(common_node));
            }
        }
        printResult(result);

        System.out.println("~~~~~~~~~~~~~~~~~~~~~ find the highway nodes at the highest level                      ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        findAtTheHighestLevel(source_node, destination_node);
        printResult(result);
        String path_name = "/home/gqxwolf/mydata/projectData/BackBone/" + source_file_folder + "/results";
        saveToDisk(path_name + "/backbone_"+source_node+"_"+destination_node+"_final_"+this.surfix+".txt");
    }

    /**
     * the informaiton of the destination node is included in the structure of the destination_to_highway_results
     *
     * @param source_node
     */
    private void findAtTheHighestLevel(long source_node, long dest_node) {
        printNodeToHighway(source_to_highway_results);
        printNodeToHighway(destination_to_highway_results);

        HashMap<Long, ArrayList<backbonePath>> all_possible_dest_node_with_skypaths = filterPossibleNodeInList(destination_to_highway_results);
        HashMap<Long, ArrayList<backbonePath>> source_list = filterPossibleNodeInList(source_to_highway_results);

        bbs.landmark_bbs(source_node, dest_node, source_list, all_possible_dest_node_with_skypaths, result);
        System.out.println("number of results sets " + this.result.size());
        System.out.println("================================================================================");
        bbs.closeDB();
    }


    /**
     * Find the result at the highest level by using the flat_index and the highest_index
     *
     * @param source_node
     */
    private void findAtTheHighestLevelFlat(long source_node, long dest_node) {
        printNodeToHighway(source_to_highway_results);
        printNodeToHighway(destination_to_highway_results);

        HashMap<Long, ArrayList<backbonePath>> all_possible_dest_node_with_skypaths = filterPossibleNodeInList(destination_to_highway_results);
        HashMap<Long, ArrayList<backbonePath>> source_list = filterPossibleNodeInList(source_to_highway_results);

        if (result.isEmpty()) {
            System.out.println("Init the result set: ==================================================================================");

            ArrayList<Long> sorted_source_list = new ArrayList<>(source_list.keySet());
            Random random = new Random();
            int random_source_idx = random.nextInt(sorted_source_list.size());
            random_source_idx = 12;
            Set<Map.Entry<Long, ArrayList<backbonePath>>> entrySet = source_list.entrySet();
            ArrayList<Map.Entry<Long, ArrayList<backbonePath>>> listOfSources = new ArrayList<>(entrySet);
            System.out.println("Choose the index with " + random_source_idx + "  ====>>>>  " + listOfSources.get(random_source_idx).getKey());
//            ArrayList<backbonePath> init_dummy_paths = bbs.initResultLandMark(listOfSources.get(random_source_idx), all_possible_dest_node_with_skypaths);
            ArrayList<backbonePath> init_dummy_paths = bbs.initResultShortestPath(listOfSources.get(random_source_idx), all_possible_dest_node_with_skypaths);
            for (backbonePath fake_path : init_dummy_paths) {
                addToSkyline(this.result, fake_path);
            }


//            for (Map.Entry<Long, ArrayList<backbonePath>> source_info_list : source_list.entrySet()) {
//                ArrayList<backbonePath> init_dummy_paths = bbs.initResultLandMark(source_info_list, all_possible_dest_node_with_skypaths);
////                    ArrayList<backbonePath> init_dummy_paths = bbs.initResultShortestPath(source_info_list, all_possible_dest_node_with_skypaths);
//
//                for (backbonePath fake_path : init_dummy_paths) {
//                    addToSkyline(this.result, fake_path);
//                }
//                break;
//
//            }

            for (backbonePath bp : result) {
                System.out.println(bp);
            }
            System.out.println("=======================================================================================================");

        }

        long bbs_rt = System.currentTimeMillis();
        bbs.landmark_bbs(source_node, dest_node, source_list, all_possible_dest_node_with_skypaths, result);
        System.out.println("number of results sets : " + this.result.size() + "   running time : " + (System.currentTimeMillis() - bbs_rt) + " ms ");
        System.out.println("================================================================================");

    }

    private HashMap<Long, ArrayList<backbonePath>> filterPossibleNodeInList(HashMap<Long, ArrayList<backbonePath>> original_list) {
        HashMap<Long, ArrayList<backbonePath>> result = new HashMap<>();
        for (Map.Entry<Long, ArrayList<backbonePath>> e : original_list.entrySet()) {
            if (this.bbs.node_list.contains(e.getKey())) {
                result.put(e.getKey(), e.getValue());
            }
        }
        return result;
    }

    private void printNodeToHighway(HashMap<Long, ArrayList<backbonePath>> source_to_highway_results) {
        System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ " + source_to_highway_results.size());
        for (Map.Entry<Long, ArrayList<backbonePath>> e : source_to_highway_results.entrySet()) {
            System.out.println(e.getKey()+"   "+bbs.node_list.contains(e.getKey()));
//            if (bbs.node_list.contains(e.getKey())) {
//                System.out.println(e.getKey());
////                for (backbonePath bp : e.getValue()) {
////                    System.out.println(e.getKey() + " ############  " + bp + "  " + bbs.node_list.contains(e.getKey()));
////                }
//            }
        }
        System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
    }


    private void findCommonLayer(long source_node, long destination_node) {
        for (int l = 0; l < this.index_level; l++) {
            HashSet<Long> src_highways = index.getHighwayNodeAtLevel(l, source_node);
            HashSet<Long> dest_highways = index.getHighwayNodeAtLevel(l, destination_node);
            System.out.println("finding the common highway of source node and destination at level " + l);
            if (src_highways != null && dest_highways != null) {
                System.out.println("# of common highways is " + findCommandHighways(src_highways, dest_highways).size());
            } else {
                boolean a = src_highways == null, b = dest_highways == null;
                System.out.println("there is no highways of " + (a ? "    src node " : "") + (b ? "    dest node" : ""));
            }
            System.out.println("=======================================================");
        }
    }

    private HashSet<Long> findCommandHighways(Set<Long> src_set, Set<Long> dest_set) {
        HashSet<Long> commonset = new HashSet<>();
        for (long s_element : src_set) {
            if (dest_set.contains(s_element)) {
                commonset.add(s_element);
            }
        }
        return commonset;
    }


    private void printResult(ArrayList<backbonePath> result) {
        for (backbonePath r : result) {
            System.out.println("the temp results:" + r);
        }
    }


    public boolean addToSkyline(ArrayList<backbonePath> bp_list, backbonePath bp) {
        monitor.callAddToSkyline++;
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

    private boolean dominatedByResult(backbonePath np) {
        monitor.callcheckdominatedbyresult++;
        monitor.allsizeofthecheckdominatedbyresult += this.result.size();
        long rt_check_dominatedByresult = System.nanoTime();
        for (backbonePath rp : this.result) {
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

    /**
     * Add the new backbone path new_bp to the skyline backbone path set that is from source_node to end node (highway node) of the new_bp
     *
     * @param new_bp
     * @param highway_results
     * @return
     */
    private boolean addToResultSet(backbonePath new_bp, HashMap<Long, ArrayList<backbonePath>> highway_results) {
        long h_node = new_bp.destination;// h_node is the highway node of next layer.
        ArrayList<backbonePath> h_list = highway_results.get(h_node); //the list of backbone paths from source node to highways.
        if (h_list != null) {
            if (addToSkyline(h_list, new_bp)) {
                highway_results.put(h_node, h_list);
                return true;
            } else {
                return false;
            }
        } else {
            h_list = new ArrayList<>();
            h_list.add(new_bp);
            highway_results.put(h_node, h_list);
            return true;
        }
    }

    public void saveToDisk(String target_path) {
        File file = new File(target_path);

        if (file.exists()) {
            file.delete();
        }

        try (FileWriter fw = new FileWriter(target_path, true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {

            for (backbonePath p : this.result) {
                out.println(p);
            }

            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void saveToDisk(ArrayList<backbonePath> temp_result, String target_path) {
        File file = new File(target_path);

        if (file.exists()) {
            file.delete();
        }

        try (FileWriter fw = new FileWriter(target_path, true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {

            for (backbonePath p : temp_result) {
                out.println(p);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void combinationResult(ArrayList<backbonePath> src_to_common_highway, ArrayList<backbonePath> dest_to_common_highway) {

//        ArrayList<backbonePath> bps_results = new ArrayList<>();

        for (backbonePath s_t_h_bpath : src_to_common_highway) {
            for (backbonePath d_t_h_bpath : dest_to_common_highway) {
                long s_combination_new_path_c_rt = System.nanoTime();
                backbonePath result_backbone = new backbonePath(s_t_h_bpath, d_t_h_bpath, true);
                long e_combination_new_path_c_rt = System.nanoTime();


                monitor.finnalCallAddToSkyline++;
                if (addToSkyline(this.result, result_backbone)) {
//                    bps_results.add(result_backbone);
                }

                long e_add_rt = System.nanoTime();
                this.monitor.runningtime_combination_addtoskyline += (e_add_rt - e_combination_new_path_c_rt);
                this.monitor.runningtime_combination_construction += (e_combination_new_path_c_rt - s_combination_new_path_c_rt);
            }
        }

//        return bps_results;
    }
}
