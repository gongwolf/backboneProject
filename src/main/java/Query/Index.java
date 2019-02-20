package Query;

import java.io.*;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;

public class Index {
    public ArrayList<Hashtable<Long, Hashtable<Long, ArrayList<double[]>>>> index = new ArrayList();  //level --> <node id --->{ highway id ==> <skyline paths > }  >
    public ArrayList<Hashtable<Long, ArrayList<Long>>> nodesToHighway_index = new ArrayList();        //level --> <source_node_id --> list of the highway>


    int graphsize;
    int dimension;
    int degree;

    String index_folder;
    String nth_folder;

    int total_level;

    ArrayList<backbonePath> result = new ArrayList<>();

    HashMap<Long, ArrayList<backbonePath>> source_to_highway_results = new HashMap<>(); //the temporary results from source node to highways
    HashMap<Long, ArrayList<backbonePath>> destination_to_highway_results = new HashMap<>(); //the temporary results from destination node to highways

    public Index(int graphsize, int dimension, int degree) {
        this.graphsize = graphsize;
        this.degree = degree;
        this.dimension = dimension;
        index_folder = "/home/gqxwolf/mydata/projectData/BackBone/indexes/backbone_" + graphsize + "_" + degree + "_" + dimension;
        nth_folder = index_folder + "/nodeToHighway_index";
        readIndexFromDisk();
    }

    public static void main(String args[]) {
        Index i = new Index(1000, 3, 4);
        i.test();
    }

    private void test() {
        long source_node = 89;
        long destination_node = 947;

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
                ArrayList<Long> highwaysOfsrcNode = this.nodesToHighway_index.get(l).get(s_id);//get highways of sid
                if (highwaysOfsrcNode != null) {
                    for (long h_node : highwaysOfsrcNode) {//h_node is highway node of the sid, it's the source node to the next level
                        ArrayList<double[]> source_to_highway_list = readHighwaysInformation(h_node, l, s_id); //
                        if (source_to_highway_list != null || !source_to_highway_list.isEmpty()) {
                            boolean needtoinserted = false;

                            ArrayList<backbonePath> bps_src_to_sid = source_to_highway_results.get(s_id);

                            for (backbonePath old_path : bps_src_to_sid) {
                                for (double[] costs : source_to_highway_list) {
                                    backbonePath new_bp = new backbonePath(h_node, costs, old_path); //the new path from the sid->old_highway->new_highway
                                    boolean flag = addToResultSet(new_bp, source_to_highway_results);

                                    if (flag && !needtoinserted) {
                                        needtoinserted = true;
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

            System.out.println("~~~~~~~~~~~~~~~~~~~~~~");

            for (long d_id : dhList) {
                ArrayList<Long> highwaysOfDestNode = this.nodesToHighway_index.get(l).get(d_id);//get highways of did
                if (highwaysOfDestNode != null) {
                    for (long h_node : highwaysOfDestNode) {//h_node is highway node of the did, it's the destination node to the next level

                        ArrayList<double[]> destination_to_highway_list = readHighwaysInformation(h_node, l, d_id);
                        if (destination_to_highway_list != null || !destination_to_highway_list.isEmpty()) {
                            boolean needtoinserted = false;
                            ArrayList<backbonePath> bps_dest_to_did = destination_to_highway_results.get(d_id);

                            for (backbonePath old_path : bps_dest_to_did) {
                                for (double[] costs : destination_to_highway_list) {
                                    backbonePath new_bp = new backbonePath(h_node, costs, old_path); //the new path from the sid->old_highway->new_highway
                                    boolean flag = addToResultSet(new_bp, destination_to_highway_results);

                                    if (flag && !needtoinserted) {
                                        needtoinserted = true;
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

            HashSet<Long> commonset = findCommandHighways(needs_to_add_to_source, needs_to_add_to_destination);

            System.out.println("commond set : " + commonset);

            if (!commonset.isEmpty()) {
                for (long common_node : commonset) {
                    combinationResult(source_to_highway_results.get(common_node), destination_to_highway_results.get(common_node));
                }
            }

            System.out.println("the result at level" + l + ":");
            printResult();
            System.out.println("======================================================================");
        }


        System.out.println("nodes to highways of the level: source " + needs_to_add_to_source);
        System.out.println("nodes to highways of the level: destination " + needs_to_add_to_destination);
        HashSet<Long> commonset = findCommandHighways(needs_to_add_to_source, needs_to_add_to_destination);
        System.out.println(commonset);
        supplementResult(commonset, source_node, destination_node, needs_to_add_to_source, needs_to_add_to_destination);


        System.out.println("the result at final level:");
        printResult();
    }

    private void supplementResult(HashSet<Long> commonset, long src, long dest, HashSet<Long> needs_to_add_to_source, HashSet<Long> needs_to_add_to_destination) {

        for (long sid : needs_to_add_to_source) {
            for (long did : needs_to_add_to_destination) {
                if (sid != did) {
                    ArrayList<backbonePath> src_to_common_highway = source_to_highway_results.get(sid);
                    ArrayList<backbonePath> dest_to_common_highway = destination_to_highway_results.get(did);

                    ArrayList<double[]> costs_sid_to_did = this.index.get(this.total_level).get(sid).get(did);

                    for (backbonePath s_t_h_bpath : src_to_common_highway) {
                        for (backbonePath d_t_h_bpath : dest_to_common_highway) {
                            for (double[] costs : costs_sid_to_did) {
                                backbonePath result_backbone = new backbonePath(s_t_h_bpath, d_t_h_bpath, costs);
                                addToSkyline(this.result, result_backbone);
                                System.out.println("suppliment result " + result_backbone);
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
                backbonePath result_backbone = new backbonePath(s_t_h_bpath, d_t_h_bpath);
//                System.out.println("the result backbone ::::::>>>>> " + result_backbone);
                if (addToSkyline(this.result, result_backbone)) {
                    bps_results.add(result_backbone);
                }
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
        int i = 0;

        if (bp_list.isEmpty()) {
            bp_list.add(bp);
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


            BigDecimal ci = new BigDecimal(String.valueOf(costs[i])).setScale(3, BigDecimal.ROUND_HALF_UP);
            BigDecimal ei = new BigDecimal(String.valueOf(estimatedCosts[i])).setScale(3, BigDecimal.ROUND_HALF_UP);

            if (ci.doubleValue() > ei.doubleValue()) {
                return false;
            }
        }
        return true;
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

    /**
     * @param h_node      highway node id
     * @param level       the level of the index
     * @param source_node the source node id
     * @return the skyline costs from the source node to the highway node
     */
    private ArrayList<double[]> readHighwaysInformation(long h_node, int level, long source_node) {
        ArrayList<double[]> source_to_highway_list = this.index.get(level).get(h_node).get(source_node);
        if (source_to_highway_list != null) {
            return source_to_highway_list;
        } else {
            return null;
        }

    }

    private void readIndexFromDisk() {
        this.total_level = getlevel();
        System.out.println("there are " + this.total_level + " level indexes");
        for (int i = 0; i <= this.total_level; i++) {
            readIndexAtLevel(i);
        }
    }

    private void readIndexAtLevel(int level) {
        String index_folder_at_level = index_folder + "/level" + level;
        System.out.println(index_folder_at_level);

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
                    costs[0] = Double.parseDouble(line.split(" ")[1]);
                    costs[1] = Double.parseDouble(line.split(" ")[2]);
                    costs[2] = Double.parseDouble(line.split(" ")[3]);

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


            this.index.add(layer_index);
            this.nodesToHighway_index.add(nodesToHighWay);


        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private int getlevel() {
        int level = 0;
        File index_dir = new File(this.index_folder);
        System.out.println(this.index_folder);
        for (File f : index_dir.listFiles(new levelFileNameFilter())) {
            String fname = f.getName();
            int c_level = Integer.parseInt(fname.substring(fname.indexOf("level") + 5, fname.length()));

            if (c_level > level) {
                level = c_level;
            }
        }
        return level;
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
