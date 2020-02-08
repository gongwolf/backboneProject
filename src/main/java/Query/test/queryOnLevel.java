package Query.test;

import Query.BackBoneIndex;
import Query.backbonePath;
import Query.landmark.LandmarkBBS;

import java.io.*;
import java.util.*;

public class queryOnLevel {
    private final int index_level;
    String system_home_folder = System.getProperty("user.home");
    HashMap<Long, ArrayList<backbonePath>> source_to_highway_results = new HashMap<>(); //the temporary results from source node to highways
    HashMap<Long, ArrayList<backbonePath>> destination_to_highway_results = new HashMap<>(); //the temporary results from destination node to highways
    String source_file_folder = "busline_sub_graph_NY";
    String index_files_folder = system_home_folder + "/mydata/projectData/BackBone/indexes/busline_sub_graph_NY";
    BackBoneIndex index;
    private long surfix = 0;
    public ArrayList<backbonePath> result = new ArrayList<>();
    private LandmarkBBS bbs;

    public static void main(String args[]) {
        queryOnLevel q = new queryOnLevel();
        for (int i = 0; i < 10; i++) {
            q.query(0, 3227l, 8222l);
        }
    }

    public queryOnLevel() {
        index = new BackBoneIndex(index_files_folder);
        this.index_level = index.total_level;

        Date date = new Date();
        this.surfix = date.getTime();
    }

    private void query(int query_level, long source_node, long destination_node) {
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

            if (l == query_level) {
                ArrayList<backbonePath> temp_result = new ArrayList<>();
                String sub_db_name = "sub_ny_USA_Level" + l;
                LandmarkBBS layer_bbs = new LandmarkBBS(sub_db_name);

                HashMap<Long, ArrayList<backbonePath>> all_possible_dest_node_with_skypaths = filterPossibleNodeInList(destination_to_highway_results, layer_bbs);
                HashMap<Long, ArrayList<backbonePath>> source_list = filterPossibleNodeInList(source_to_highway_results, layer_bbs);

                System.out.println("BBS query on the graph " + layer_bbs.neo4j.graphDB);
                System.out.println("source list : " + source_list.keySet());
                System.out.println("destination list : " + all_possible_dest_node_with_skypaths.keySet());
                layer_bbs.readLandmarkIndex(3, null, false);

                ArrayList<Long> sorted_source_list = new ArrayList<>(source_list.keySet());
                Random random = new Random();
                int random_source_idx = random.nextInt(sorted_source_list.size());
                Set<Map.Entry<Long, ArrayList<backbonePath>>> entrySet = source_list.entrySet();
                ArrayList<Map.Entry<Long, ArrayList<backbonePath>>> listOfSources = new ArrayList<>(entrySet);
                System.out.println("Choose the index with " + random_source_idx + "  ====>>>>  " + listOfSources.get(random_source_idx).getKey());
                ArrayList<backbonePath> layer_init_paths = layer_bbs.initResultShortestPath(listOfSources.get(random_source_idx), all_possible_dest_node_with_skypaths);
                for (backbonePath init_bp : layer_init_paths) {
                    addToSkyline(temp_result, init_bp);
                }

                long rt = System.currentTimeMillis();
                layer_bbs.landmark_bbs(source_node, destination_node, source_list, all_possible_dest_node_with_skypaths, temp_result);
                String path_name = "/home/gqxwolf/mydata/projectData/BackBone/" + source_file_folder + "/results";
                System.out.println("The BBS query at level " + l + " found " + temp_result.size() + "  skyline paths  in " + (System.currentTimeMillis() - rt) + "ms");
                saveToDisk(temp_result, path_name + "/backbone_" + source_node + "_" + destination_node + "_query_on_level" + l + "_" + this.surfix + ".txt");
                layer_bbs.closeDB();
                System.out.println("==================================================================================");
                break;
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
                                    }
                                }
                            } else {
                                ArrayList<backbonePath> bps_src_to_sid = source_to_highway_results.get(s_id);
                                for (backbonePath old_path : bps_src_to_sid) {
                                    for (double[] costs : cost_from_src_to_highway) {
                                        backbonePath new_bp = new backbonePath(h_node, costs, old_path); //the new path from the sid->old_highway->new_highway
//                                        System.out.println(s_id + "   " + h_node + " : " + old_path + " =====================> " + new_bp + " --> " + new_bp.hasCycle);

                                        if (!dominatedByResult(new_bp) && !new_bp.hasCycle) {
                                            boolean flag = addToResultSet(new_bp, source_to_highway_results);
                                        }
                                    }
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
                                    }
                                }
                            } else {
                                ArrayList<backbonePath> bps_dest_to_did = destination_to_highway_results.get(d_id); //the backbone paths from destination to did
                                for (backbonePath old_path : bps_dest_to_did) {
                                    for (double[] costs : cost_from_dest_to_highway) {
                                        backbonePath new_bp = new backbonePath(h_node, costs, old_path); //the new path from the destination->old_highway->new_highway
//                                        System.out.println(h_node + "     " + d_id + " : " + old_path + " =====================> " + new_bp + " --> " + new_bp.hasCycle);
                                        if (!dominatedByResult(new_bp) && !new_bp.hasCycle) {
                                            boolean flag = addToResultSet(new_bp, destination_to_highway_results);
                                        }
                                    }
                                }
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


    private HashMap<Long, ArrayList<backbonePath>> filterPossibleNodeInList(HashMap<Long, ArrayList<backbonePath>> original_list, LandmarkBBS layer_bbs) {
        HashMap<Long, ArrayList<backbonePath>> result = new HashMap<>();
        for (Map.Entry<Long, ArrayList<backbonePath>> e : original_list.entrySet()) {
            if (layer_bbs.node_list.contains(e.getKey())) {
                result.put(e.getKey(), e.getValue());
            }
        }
        return result;
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

    private boolean checkDominated(double[] costs, double[] estimatedCosts) {
        for (int i = 0; i < costs.length; i++) {
            if (costs[i] > estimatedCosts[i]) {
                return false;
            }
        }

        return true;
    }

    private void printResult(ArrayList<backbonePath> result) {
        for (backbonePath r : result) {
            System.out.println("the temp results:" + r);
        }
    }

    private boolean dominatedByResult(backbonePath np) {
        for (backbonePath rp : this.result) {
            if (checkDominated(rp.costs, np.costs)) {
                return true;
            }
        }

        return false;
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
}
