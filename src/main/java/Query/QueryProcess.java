package Query;

import Baseline.BBSBaselineBusline;
import Baseline.path;
import DataStructure.Monitor;
import Neo4jTools.Neo4jDB;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.*;

public class QueryProcess {
    private final int index_level;
    private final Monitor monitor;

    String system_home_folder = System.getProperty("user.home");
    String index_files_folder = system_home_folder + "/mydata/projectData/BackBone/indexes/busline_sub_graph_NY";
    BackBoneIndex index;

    HashMap<Long, ArrayList<backbonePath>> source_to_highway_results = new HashMap<>(); //the temporary results from source node to highways
    HashMap<Long, ArrayList<backbonePath>> destination_to_highway_results = new HashMap<>(); //the temporary results from destination node to highways

    public ArrayList<backbonePath> result = new ArrayList<>();
    BBSBaselineBusline bbs;

    public QueryProcess() {
        index = new BackBoneIndex(index_files_folder);
        this.index_level = index.total_level;

        String sub_db_name = "sub_ny_USA_Level" + this.index_level;
        bbs = new BBSBaselineBusline(sub_db_name);
//        bbs.buildLandmarkIndex(1);
        this.monitor = new Monitor();

    }

    public static void main(String args[]) {
        QueryProcess query = new QueryProcess();
        long running_time = System.nanoTime();
        query.query(3227, 8222);
        System.out.println("Total Runningt time is " + (System.nanoTime() - running_time) / 1000000 + " ms ");
    }

    private void query(long source_node, long destination_node) {
        System.out.println("=================================================================================");
        findCommonLayer(source_node, destination_node);
        System.out.println("=================================================================================");

        backbonePath sourceDummyResult = new backbonePath(source_node);
        ArrayList<backbonePath> temp_src_list = new ArrayList<>();
        temp_src_list.add(sourceDummyResult);
        source_to_highway_results.put(source_node, temp_src_list);

        //the destination to it self
        backbonePath destDummyResult = new backbonePath(destination_node);
        ArrayList<backbonePath> temp_dest_list = new ArrayList<>();
        temp_dest_list.add(destDummyResult);
        destination_to_highway_results.put(destination_node, temp_dest_list);

        HashSet<Long> needs_to_add_to_source = new HashSet<>();
        HashSet<Long> needs_to_add_to_destination = new HashSet<>();


        for (int l = 0; l < this.index_level; l++) {
            System.out.println("Find the index information at level " + l);
            System.out.println("==================================================================================");
            needs_to_add_to_source.clear();
            needs_to_add_to_destination.clear();

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
                                boolean needtoinserted = false;
                                ArrayList<backbonePath> bps_src_to_sid = source_to_highway_results.get(s_id);
                                for (backbonePath old_path : bps_src_to_sid) {
                                    for (double[] costs : cost_from_src_to_highway) {
                                        long s_creat_rt_src = System.nanoTime();
                                        backbonePath new_bp = new backbonePath(h_node, costs, old_path); //the new path from the sid->old_highway->new_highway
                                        System.out.println(s_id + "   " + h_node + " : " + old_path + " =====================> " + new_bp + " --> " + new_bp.hasCycle);
                                        long e_creat_rt_src = System.nanoTime();
                                        monitor.runningtime_src_create_newpath += (e_creat_rt_src - s_creat_rt_src);

                                        if (!dominatedByResult(new_bp) && !new_bp.hasCycle) {
                                            long s_add_rt_src = System.nanoTime();
                                            boolean flag = addToResultSet(new_bp, source_to_highway_results);
                                            long e_add_rt_src = System.nanoTime();
                                            monitor.runningtime_src_addtoskyline += (e_add_rt_src - s_add_rt_src);
                                            if (flag && !needtoinserted) {
                                                needtoinserted = true;
                                            }
                                        }
                                    }
                                }

                                if (needtoinserted) {
                                    needs_to_add_to_source.add(h_node);
                                }
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
                                boolean needtoinserted = false;
                                ArrayList<backbonePath> bps_dest_to_did = destination_to_highway_results.get(d_id); //the backbone paths from destination to did
                                for (backbonePath old_path : bps_dest_to_did) {

                                    for (double[] costs : cost_from_dest_to_highway) {
                                        long s_creat_rt_dest = System.nanoTime();
                                        backbonePath new_bp = new backbonePath(h_node, costs, old_path); //the new path from the destination->old_highway->new_highway
                                        System.out.println(h_node + "     " + d_id + " : " + old_path + " =====================> " + new_bp + " --> " + new_bp.hasCycle);
                                        long e_creat_rt_dest = System.nanoTime();
                                        monitor.runningtime_dest_create_newpath += (e_creat_rt_dest - s_creat_rt_dest);
                                        if (!dominatedByResult(new_bp) && !new_bp.hasCycle) {
                                            long s_add_rt_dest = System.nanoTime();
                                            boolean flag = addToResultSet(new_bp, destination_to_highway_results);
                                            long e_add_rt_dest = System.nanoTime();
                                            monitor.runningtime_dest_addtoskyline += (e_add_rt_dest - s_add_rt_dest);

                                            if (flag && !needtoinserted) {
                                                needtoinserted = true;
                                            }
                                        }
                                    }
                                }


                                if (needtoinserted) {
                                    needs_to_add_to_destination.add(h_node);
                                }
                            }
                        }
                    }
                }
            }
            System.out.println("the result at level" + l + ":");
            printResult();
            System.out.println("======================================================================");
        }

//        System.out.println("~~~~~~~~~~~~~~~~~~~~~ find the common highway nodes at lower level (except the highest level) ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
//        HashSet<Long> commonset = findCommandHighways(source_to_highway_results.keySet(), destination_to_highway_results.keySet());
//        if (!commonset.isEmpty()) {
//            for (long common_node : commonset) {
//                ArrayList<backbonePath> temp_combined_results = combinationResult(source_to_highway_results.get(common_node), destination_to_highway_results.get(common_node));
//                for (backbonePath pp : temp_combined_results) {
//                    System.out.println(pp);
//                }
//            }
//        }
//        printResult();
//        System.out.println("~~~~~~~~~~~~~~~~~~~~~ find the common highway nodes at the highest level                      ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
//        findAtTheHighestLevel();
//        printResult();
    }

    private void findAtTheHighestLevel() {
        printNodeToHighway(source_to_highway_results);
        printNodeToHighway(destination_to_highway_results);

        for (Map.Entry<Long, ArrayList<backbonePath>> source_info_list : source_to_highway_results.entrySet()) {
            long source_node = source_info_list.getKey();

            for (Map.Entry<Long, ArrayList<backbonePath>> dest_info_list : destination_to_highway_results.entrySet()) {
                long dest_node = dest_info_list.getKey();
                ArrayList<path> bbs_result = bbs.queryOnline(source_node, dest_node);

                for (backbonePath source_bp : source_info_list.getValue()) {
                    for (backbonePath dest_bp : dest_info_list.getValue()) {
                        for (path bbs_p : bbs_result) {
                            backbonePath final_backbone_path = new backbonePath(source_bp, bbs_p, dest_bp);
                            addToSkyline(result, final_backbone_path);
                        }
                    }
                }
            }
        }

        bbs.closeDB();

    }

    private void printNodeToHighway(HashMap<Long, ArrayList<backbonePath>> source_to_highway_results) {
        System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ " + source_to_highway_results.size());
        for (Map.Entry<Long, ArrayList<backbonePath>> e : source_to_highway_results.entrySet()) {
            for (backbonePath bp : e.getValue()) {
                System.out.println(e + "   " + bp);
            }
        }
        System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
    }


    private void findCommonLayer(long source_node, long destination_node) {
        for (int l = 0; l < this.index_level; l++) {
            HashSet<Long> src_highways = index.getHighwayNodeAtLevel(l, source_node);
            HashSet<Long> dest_highways = index.getHighwayNodeAtLevel(l, destination_node);

            System.out.println("finding the common highway of source node and destination at level " + l);

            if (src_highways != null && dest_highways != null) {
                System.out.println(findCommandHighways(src_highways, dest_highways).size());
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


    private void printResult() {
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
            if (costs[i] * (1) > estimatedCosts[i]) {
                return false;
            }
        }
        return true;
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


    private ArrayList<backbonePath> combinationResult(ArrayList<backbonePath> src_to_common_highway, ArrayList<backbonePath> dest_to_common_highway) {

        ArrayList<backbonePath> bps_results = new ArrayList<>();

        for (backbonePath s_t_h_bpath : src_to_common_highway) {
            for (backbonePath d_t_h_bpath : dest_to_common_highway) {
                long s_combination_new_path_c_rt = System.nanoTime();
                backbonePath result_backbone = new backbonePath(s_t_h_bpath, d_t_h_bpath);
                long e_combination_new_path_c_rt = System.nanoTime();


                monitor.finnalCallAddToSkyline++;
                if (addToSkyline(this.result, result_backbone)) {
                    bps_results.add(result_backbone);
                }

                long e_add_rt = System.nanoTime();
                this.monitor.runningtime_combination_addtoskyline += (e_add_rt - e_combination_new_path_c_rt);
                this.monitor.runningtime_combination_construction += (e_combination_new_path_c_rt - s_combination_new_path_c_rt);
            }
        }

        return bps_results;
    }
}