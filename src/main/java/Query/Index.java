package Query;

import DataStructure.Monitor;

import java.io.*;
import java.math.BigDecimal;
import java.util.*;

public class Index {
    public ArrayList<Hashtable<Long, Hashtable<Long, ArrayList<double[]>>>> index = new ArrayList();  //level --> <node id --->{ highway id ==> <skyline paths > }  >
    public ArrayList<Hashtable<Long, ArrayList<Long>>> nodesToHighway_index = new ArrayList();        //level --> <source_node_id --> list of the highway>
    public ArrayList<Hashtable<Long, Hashtable<Long, ArrayList<double[]>>>> intra_index = new ArrayList();  //level --> <node id --->{ highway id ==> <skyline paths > }  >


    public Monitor monitor;
    public ArrayList<backbonePath> result = new ArrayList<>();
    long graphsize;
    int dimension;
    int degree;
    String index_folder;
    String nth_folder;
    int total_level;
    HashMap<Long, ArrayList<backbonePath>> source_to_highway_results = new HashMap<>(); //the temporary results from source node to highways
    HashMap<Long, ArrayList<backbonePath>> destination_to_highway_results = new HashMap<>(); //the temporary results from destination node to highways


    public Index(long graphsize, int dimension, int degree, boolean readIntraIndex) {
        this.graphsize = graphsize;
        this.degree = degree;
        this.dimension = dimension;
        index_folder = "/home/gqxwolf/mydata/projectData/BackBone/indexes/backbone_" + graphsize + "_" + degree + "_" + dimension;
        nth_folder = index_folder + "/nodeToHighway_index";
        readIndexFromDisk(readIntraIndex);
        this.monitor = new Monitor();
    }

    public static void main(String args[]) {
        long start_ms = System.currentTimeMillis();
        boolean readIntraIndex = true;
        Index i = new Index(30, 3, 3, readIntraIndex);
        long running_start_ms = System.currentTimeMillis();

        i.test(16, 22, readIntraIndex);
//        long end_ms = System.currentTimeMillis();
//        System.out.println("running time (include index reading ): " + (end_ms - start_ms) + " ms");
//        System.out.println("running time: " + (end_ms - running_start_ms) + " ms");
//        System.out.println(i.monitor.callAddToSkyline + "   " + i.monitor.finnalCallAddToSkyline);
    }

    public void test(long source_node, long destination_node, boolean useIntraIndex) {

//        findCommonLayer(source_node, destination_node);


        //the source node to it self
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

        for (int l = 0; l <= this.total_level; l++) {
            System.out.println("Find the index information at level " + l);
            needs_to_add_to_source.clear();
            needs_to_add_to_destination.clear();

            HashSet<Long> shList = new HashSet<>(source_to_highway_results.keySet());
            HashSet<Long> dhList = new HashSet<>(destination_to_highway_results.keySet());

            for (long s_id : shList) {
                ArrayList<Long> highwaysOfsrcNode = this.nodesToHighway_index.get(l).get(s_id);//get highways of s_id
                if (highwaysOfsrcNode != null) {
                    for (long h_node : highwaysOfsrcNode) {//h_node is highway node of the sid, it's the source node to the next level
                        ArrayList<double[]> source_to_highway_list = readHighwaysInformation(h_node, l, s_id);
                        if (source_to_highway_list != null || !source_to_highway_list.isEmpty()) {
                            if (h_node == destination_node) {
                                ArrayList<backbonePath> bps_src_to_sid = source_to_highway_results.get(s_id); // the backbone paths from source node to s_id;
                                for (backbonePath old_path : bps_src_to_sid) {
                                    for (double[] costs : source_to_highway_list) {
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
                                    for (double[] costs : source_to_highway_list) {
                                        long s_creat_rt_src = System.nanoTime();
                                        backbonePath new_bp = new backbonePath(h_node, costs, old_path); //the new path from the sid->old_highway->new_highway
                                        System.out.println(s_id + "   " + h_node + " : " + old_path + "  " + new_bp + " --> " + new_bp.hasCycle);

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
                ArrayList<Long> highwaysOfDestNode = this.nodesToHighway_index.get(l).get(d_id);//get highways of did
                if (highwaysOfDestNode != null) {
                    for (long h_node : highwaysOfDestNode) {//h_node is highway node of the did, it's the destination node to the next level
                        ArrayList<double[]> destination_to_highway_list = readHighwaysInformation(h_node, l, d_id); //costs from did to h_node
                        if (destination_to_highway_list != null || !destination_to_highway_list.isEmpty()) {
                            if (h_node == source_node) {
                                ArrayList<backbonePath> bps_dest_to_did = destination_to_highway_results.get(d_id); // the backbone paths from destination node to d_id;
                                for (backbonePath old_path : bps_dest_to_did) {
                                    for (double[] costs : destination_to_highway_list) {
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
                                    for (double[] costs : destination_to_highway_list) {
                                        long s_creat_rt_dest = System.nanoTime();
                                        backbonePath new_bp = new backbonePath(h_node, costs, old_path); //the new path from the destination->old_highway->new_highway
                                        System.out.println(h_node + "     " + d_id + " : " + old_path + " --> " + new_bp);

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

//            System.out.println("intra-index ============================== ");

            if (useIntraIndex) {
                for (long src_key : source_to_highway_results.keySet()) {
                    if (intra_index.get(l).get(src_key) != null) {
                        Hashtable<Long, ArrayList<double[]>> src_dest_list = intra_index.get(l).get(src_key);
                        for (long dest_key : destination_to_highway_results.keySet()) {
                            if (src_dest_list.get(dest_key) != null) {
//                                System.out.println(src_key + "------------>" + dest_key);
                                ArrayList<backbonePath> src_to_hw_list = source_to_highway_results.get(src_key);
                                ArrayList<backbonePath> hw_to_dest_list = destination_to_highway_results.get(dest_key);

                                for (double[] costs : src_dest_list.get(dest_key)) {
                                    for (backbonePath bp_src : src_to_hw_list) {
                                        for (backbonePath dest_bp : hw_to_dest_list) {
                                            backbonePath result_backbone = new backbonePath(bp_src, dest_bp, costs);
                                            addToSkyline(this.result, result_backbone);
                                        }
                                    }
                                }

                            }
                        }

                    }
                }
            }

            HashSet<Long> commonset = findCommandHighways(source_to_highway_results.keySet(), destination_to_highway_results.keySet());
//            System.out.println("commond set : " + commonset);
            if (!commonset.isEmpty()) {
                for (long common_node : commonset) {
                    combinationResult(source_to_highway_results.get(common_node), destination_to_highway_results.get(common_node));
                }
            }
            System.out.println("the result at level" + l + ":");
            printResult();
//
//            System.out.println("=======-----------------------=======");
//
//            for (Map.Entry<Long, ArrayList<backbonePath>> sh : source_to_highway_results.entrySet()) {
//                long shid = sh.getKey();
//                System.out.println(shid);
//                for (backbonePath bp : sh.getValue()) {
//                    System.out.println("    " + bp);
//                }
//            }
//
//            System.out.println("-------------------------------------------------------------------------------------------------------");
//
//            for (Map.Entry<Long, ArrayList<backbonePath>> dh : destination_to_highway_results.entrySet()) {
//                long dhid = dh.getKey();
//                System.out.println(dhid);
//                for (backbonePath bp : dh.getValue()) {
//                    System.out.println("    " + bp);
//                }
//            }

            System.out.println("======================================================================");
        }
//        System.out.println(monitor.callAddToSkyline + "     " + monitor.finnalCallAddToSkyline);

        HashSet<Long> commonset = findCommandHighways(needs_to_add_to_source, needs_to_add_to_destination);
        supplementHighestLevel(needs_to_add_to_source, needs_to_add_to_destination);


//        System.out.println("======= find the temp-results");
        supplementAllIndex();

//        printResult();
//        System.out.println(monitor.getRunningtime_supplement_addtoskylineByms() + "     " + monitor.getRunningtime_supplement_constructionByms());
//        System.out.println(monitor.getRunningtime_combination_addtoskylineByms() + "     " + monitor.getRunningtime_combination_constructionByms());
//        System.out.println(monitor.getRunningtime_src_addtoskylineByms() + "     " + monitor.getRunningtime_dest_addtoskylineByms() + "  " + monitor.getRunningtime_intermedia_addtoskyline());
//        System.out.println(monitor.getRunningtime_src_createlineByms() + "     " + monitor.getRunningtime_dest_createByms() + "  " + monitor.getRunningtime_intermedia_createline());
//        System.out.println(monitor.getRunningtime_check_domination_resultByms());
//        System.out.println(monitor.callcheckdominatedbyresult);
//        System.out.println(monitor.allsizeofthecheckdominatedbyresult);
//        System.out.println("the result at final level:");
//        printResult();
    }

    private void supplementAllIndex() {
        for (Map.Entry<Long, ArrayList<backbonePath>> sh : source_to_highway_results.entrySet()) {
            long shid = sh.getKey();
//            System.out.println(shid);
//            for (backbonePath bp : sh.getValue()) {
//                System.out.println(" " + bp);
//            }
            for (Map.Entry<Long, ArrayList<backbonePath>> dh : destination_to_highway_results.entrySet()) {
                long dhid = dh.getKey();
//                System.out.println("    [" + dhid + "]");

                if (dhid != shid) {
                    ArrayList<double[]> costs = findLinkBackbonePath(shid, dhid);
                    if (!costs.isEmpty()) {
                        for (double[] c : costs) {
                            backbonePath tmp_src_dest_bp = new backbonePath(shid, dhid, c);
                            if (!dominatedByResult(tmp_src_dest_bp)) {
                                for (backbonePath s_t_h_bpath : sh.getValue()) {
                                    for (backbonePath d_t_h_bpath : dh.getValue()) {
                                        backbonePath result_backbone = new backbonePath(s_t_h_bpath, d_t_h_bpath, c);
                                        addToSkyline(this.result, result_backbone);

                                    }
                                }
                            }
                        }

                    }
                }
            }
        }
    }

    private ArrayList<double[]> findLinkBackbonePath(long shid, long dhid) {
        ArrayList<double[]> result = new ArrayList<>();
//        System.out.println("    find     " + shid + "   " + dhid);
        int i = 0;
        for (Hashtable<Long, Hashtable<Long, ArrayList<double[]>>> l_index : this.index) {
            Hashtable<Long, ArrayList<double[]>> s = l_index.get(shid);
//            Hashtable<Long, ArrayList<double[]>> d = l_index.get(dhid);
            if (s != null) {
                ArrayList<double[]> stod = s.get(dhid);
                if (stod != null) {
                    for (double costs[] : stod) {
//                        System.out.println("    " + shid + "---->" + dhid + " [" + costs[0] + "," + costs[1] + "," + costs[2] + "]    at level " + i);
                        result.add(costs);
                    }
                }
            }
            i++;
//            if (d != null) {
//                ArrayList<double[]> dtos = d.get(shid);
//                if (dtos != null) {
//                    for (double costs[] : dtos) {
//                        System.out.println("    "+dhid + "---->" + shid + " [" + costs[0] + "," + costs[1] + "," + costs[2] + "]");
//                    }
//                }
//            }
        }

        return result;
    }

    private void findCommonLayer(long source_node, long destination_node) {
        for (int l = 0; l <= this.total_level; l++) {
            ArrayList<Long> src_highways = this.nodesToHighway_index.get(l).get(source_node);
            ArrayList<Long> dest_highways = this.nodesToHighway_index.get(l).get(destination_node);

            System.out.println("common highway on level " + l);
            if (src_highways != null && dest_highways != null) {
                HashSet<Long> highwaysOfsrcNode = new HashSet<>(src_highways);
                HashSet<Long> highwaysOfdestNode = new HashSet<>(dest_highways);
                System.out.println(findCommandHighways(highwaysOfsrcNode, highwaysOfdestNode));
            } else {
                boolean a = false, b = false;
                if (src_highways == null) {
                    a = true;
                }

                if (dest_highways == null) {
                    b = true;
                }
                System.out.println("there is no highways of " + (a ? "src node " : "") + "   " + (b ? " dest node" : ""));
            }
            System.out.println("=======================================================");
        }
    }

    private void supplementHighestLevel(HashSet<Long> needs_to_add_to_source, HashSet<Long> needs_to_add_to_destination) {

        for (long sid : needs_to_add_to_source) {
            for (long did : needs_to_add_to_destination) {
                if (sid != did) {
                    ArrayList<backbonePath> src_to_common_highway = source_to_highway_results.get(sid);
                    ArrayList<backbonePath> dest_to_common_highway = destination_to_highway_results.get(did);

                    ArrayList<double[]> costs_sid_to_did = this.index.get(this.total_level).get(sid).get(did);

                    if (costs_sid_to_did == null) {
                        continue;
                    }

                    for (backbonePath s_t_h_bpath : src_to_common_highway) {
                        for (backbonePath d_t_h_bpath : dest_to_common_highway) {
                            for (double[] costs : costs_sid_to_did) {
                                backbonePath tmp_src_dest_bp = new backbonePath(sid, did, costs);
                                if (!dominatedByResult(tmp_src_dest_bp)) {
                                    long s_supplement_new_path_c_rt = System.nanoTime();
                                    backbonePath result_backbone = new backbonePath(s_t_h_bpath, d_t_h_bpath, costs);
                                    long s_add_rt = System.nanoTime();
                                    addToSkyline(this.result, result_backbone);
//                                    System.out.println("the results found by the supplement function   " + result_backbone);

                                    long e_add_rt = System.nanoTime();
                                    this.monitor.finnalCallAddToSkyline++;
                                    this.monitor.runningtime_supplement_addtoskyline += (e_add_rt - s_add_rt);
                                    this.monitor.runningtime_supplement_construction += (s_add_rt - s_supplement_new_path_c_rt);
                                }
                            }
                        }

                    }
                }
            }
        }

    }

    private void printResult() {
        for (backbonePath r : result) {
            System.out.println("the temp results:" + r);
        }
    }

    private ArrayList<backbonePath> combinationResult(ArrayList<backbonePath> src_to_common_highway, ArrayList<backbonePath> dest_to_common_highway) {

        ArrayList<backbonePath> bps_results = new ArrayList<>();

        for (backbonePath s_t_h_bpath : src_to_common_highway) {
            for (backbonePath d_t_h_bpath : dest_to_common_highway) {
                long s_combination_new_path_c_rt = System.nanoTime();
                backbonePath result_backbone = new backbonePath(s_t_h_bpath, d_t_h_bpath);
                long e_combination_new_path_c_rt = System.nanoTime();
//                System.out.println("======  " + result_backbone);


                monitor.finnalCallAddToSkyline++;
                if (addToSkyline(this.result, result_backbone)) {
                    bps_results.add(result_backbone);
//                    System.out.println(s_t_h_bpath);
//                    System.out.println(d_t_h_bpath);
//                    System.out.println(result_backbone);
//                    System.out.println("###################");
                }

                long e_add_rt = System.nanoTime();
                this.monitor.runningtime_combination_addtoskyline += (e_add_rt - e_combination_new_path_c_rt);
                this.monitor.runningtime_combination_construction += (e_combination_new_path_c_rt - s_combination_new_path_c_rt);
            }
        }

        return bps_results;
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

//        for (int i = 0; i < costs.length; i++) {
//            BigDecimal ci = new BigDecimal(String.valueOf(costs[i])).setScale(3, BigDecimal.ROUND_HALF_UP);
//            BigDecimal ei = new BigDecimal(String.valueOf(estimatedCosts[i])).setScale(3, BigDecimal.ROUND_HALF_UP);
//            if (ci.doubleValue() > ei.doubleValue()) {
//                return false;
//            }
//        }
//        return true;
    }

    private void commbinationIndex(HashSet<Long> src, HashSet<Long> dest) {

        HashSet<Long> commonset = new HashSet<>();


        for (long highway_node : src) {
            if (dest.contains(highway_node)) {
                commonset.add(highway_node);
            }
        }

        for (long h_node : commonset) {

        }


    }

    private HashSet<Long> findCommandHighways(HashSet<Long> src_set, HashSet<Long> dest_set) {
        HashSet<Long> commonset = new HashSet<>();

        for (long s_element : src_set) {
            if (dest_set.contains(s_element)) {
                commonset.add(s_element);
            }
        }

        return commonset;
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

    /**
     * @param h_node      highway node id
     * @param level       the level of the index
     * @param source_node the source node id
     * @return the skyline costs from the source node to the highway node
     */
    private ArrayList<double[]> readHighwaysInformation(long h_node, int level, long source_node) {
        ArrayList<double[]> source_to_highway_list = this.index.get(level).get(h_node).get(source_node); // get the skyline costs from source node to h_node
        if (source_to_highway_list != null) {
            return source_to_highway_list;
        } else {
            return null;
        }

    }

    private void readIndexFromDisk(boolean readIntraIndex) {
        this.total_level = getlevel();
//        System.out.println("there are " + this.total_level + " level indexes");
        for (int i = 0; i <= this.total_level; i++) {
            readIndexAtLevel(i, readIntraIndex);
        }
    }

    private void readIndexAtLevel(int level, boolean readIntraIndex) {
        String index_folder_at_level = index_folder + "/level" + level;

        File index_folder_at_level_dir = new File(index_folder_at_level);
        Hashtable<Long, Hashtable<Long, ArrayList<double[]>>> layer_index = new Hashtable<>();

        for (File idx_file : index_folder_at_level_dir.listFiles(new idxFileNameFilter())) {
            String filename = idx_file.getName();
            long highway_node = Long.parseLong(filename.substring(0, filename.lastIndexOf(".idx")));

            try (BufferedReader br = new BufferedReader(new FileReader(idx_file))) {
                String line;
                while ((line = br.readLine()) != null) {
//                    System.out.println(line);

                    long source_node = Long.parseLong(line.split(" ")[0]);

                    double[] costs = new double[dimension];
//                    costs[0] = Double.parseDouble(line.split(" ")[1]);
//                    costs[1] = Double.parseDouble(line.split(" ")[2]);
//                    costs[2] = Double.parseDouble(line.split(" ")[3]);

                    costs[0] = new BigDecimal(String.valueOf(line.split(" ")[1])).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
                    costs[1] = new BigDecimal(String.valueOf(line.split(" ")[2])).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
                    costs[2] = new BigDecimal(String.valueOf(line.split(" ")[3])).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();

                    Hashtable<Long, ArrayList<double[]>> highway_index;

                    if (!layer_index.containsKey(highway_node)) {
                        ArrayList<double[]> skyline_index = new ArrayList<>();
                        skyline_index.add(costs);
                        highway_index = new Hashtable<>();
                        highway_index.put(source_node, skyline_index);
                    } else {
                        highway_index = layer_index.get(highway_node);
                        if (!highway_index.containsKey(source_node)) {
                            ArrayList<double[]> skyline_index = new ArrayList<>();
                            skyline_index.add(costs);
                            highway_index.put(source_node, skyline_index);
                        } else {
                            ArrayList<double[]> skyline_index = highway_index.get(source_node);
                            skyline_index.add(costs);
                            highway_index.put(source_node, skyline_index);
                        }
                    }

                    layer_index.put(highway_node, highway_index);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        this.index.add(layer_index);


        if (readIntraIndex) {
            String intra_index_folder_at_level = index_folder + "/intraLayer/level" + level;

            File intra_index_folder_at_level_dir = new File(intra_index_folder_at_level);
            Hashtable<Long, Hashtable<Long, ArrayList<double[]>>> intra_layer_index = new Hashtable<>();

            for (File idx_file : intra_index_folder_at_level_dir.listFiles(new idxFileNameFilter())) {
                String filename = idx_file.getName();
                long source_node = Long.parseLong(filename.substring(0, filename.lastIndexOf(".idx")));

                try (BufferedReader br = new BufferedReader(new FileReader(idx_file))) {
                    String line;
                    while ((line = br.readLine()) != null) {

                        long destination_node = Long.parseLong(line.split(" ")[0]);

                        double[] costs = new double[dimension];

                        costs[0] = new BigDecimal(String.valueOf(line.split(" ")[1])).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
                        costs[1] = new BigDecimal(String.valueOf(line.split(" ")[2])).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
                        costs[2] = new BigDecimal(String.valueOf(line.split(" ")[3])).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();

                        Hashtable<Long, ArrayList<double[]>> src_dest_index;

                        if (!intra_layer_index.containsKey(source_node)) {
                            ArrayList<double[]> skyline_index = new ArrayList<>();
                            skyline_index.add(costs);
                            src_dest_index = new Hashtable<>();
                            src_dest_index.put(destination_node, skyline_index);
                        } else {
                            src_dest_index = intra_layer_index.get(source_node);
                            if (!src_dest_index.containsKey(destination_node)) {
                                ArrayList<double[]> skyline_index = new ArrayList<>();
                                skyline_index.add(costs);
                                src_dest_index.put(destination_node, skyline_index);
                            } else {
                                ArrayList<double[]> skyline_index = src_dest_index.get(destination_node);
                                skyline_index.add(costs);
                                src_dest_index.put(destination_node, skyline_index);
                            }
                        }

                        intra_layer_index.put(source_node, src_dest_index);
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            this.intra_index.add(intra_layer_index);

        }


        String nodes_to_highway_index_file_path = index_folder + "/nodeToHighway_index/source_to_highway_index_level" + level + ".idx";

        File nodes_to_highway_index_file = new File(nodes_to_highway_index_file_path);

        Hashtable<Long, ArrayList<Long>> nodesToHighWay = new Hashtable<>();

        try (BufferedReader br = new BufferedReader(new FileReader(nodes_to_highway_index_file))) {
            String line;
            while ((line = br.readLine()) != null) {
//                System.out.println(line);

                long source_node_id = Long.parseLong(line.split(":")[0]);
                String highway_nodes = line.split(":")[1];

                for (String highway_str : highway_nodes.split(" ")) {
                    long highway = Long.parseLong(highway_str);
                    if (!nodesToHighWay.containsKey(source_node_id)) {
                        ArrayList<Long> highways = new ArrayList<>();
                        highways.add(highway);
                        nodesToHighWay.put(source_node_id, highways);
                    } else {
                        ArrayList<Long> highways = nodesToHighWay.get(source_node_id);
                        if (!highways.contains(highway)) {
                            highways.add(highway);
                            nodesToHighWay.put(source_node_id, highways);
                        }
                    }
                }
            }


            this.nodesToHighway_index.add(nodesToHighWay);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private int getlevel() {
        int level = 0;
        File index_dir = new File(this.index_folder);
//        System.out.println(this.index_folder);
        for (File f : index_dir.listFiles(new levelFileNameFilter())) {
            String fname = f.getName();
            int c_level = Integer.parseInt(fname.substring(fname.indexOf("level") + 5, fname.length()));

            if (c_level > level) {
                level = c_level;
            }
        }
        return level;
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

    public static class levelFileNameFilter implements FilenameFilter {
        @Override
        public boolean accept(File dir, String name) {
            return name.toLowerCase().startsWith("level");
        }

    }


    public static class idxFileNameFilter implements FilenameFilter {
        @Override
        public boolean accept(File dir, String name) {
            return name.toLowerCase().endsWith(".idx");
        }

    }
}
