package v4;

import DataStructure.Monitor;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class Query {
    private int level;
    private String indexFolder;
    private int graphsize;
    private int degree;
    private int dimension;
    String homefolder = System.getProperty("user.home");
    private ArrayList<HashMap<Long, HashMap<Long, ArrayList<double[]>>>> index = new ArrayList<>(); // < level--> < node--> <highway_nodes, list<skyline paths>>>>
    public ArrayList<backbonePath> result = new ArrayList<>();
    Monitor monitor = new Monitor();

    public static void main(String args[]) {
        int graphesize = 10000;
        int degree = 4;
        int dimension = 3;
        double p =0.01;

        Query q = new Query(graphesize, degree, dimension, p);
        q.queryBetween(1, graphesize - 1);
    }


    public Query(int graphsize, int degree, int dimension, double p) {
        this.graphsize = graphsize;
        this.degree = degree;
        this.dimension = dimension;
        if (p == -1) {
            indexFolder = homefolder + "/mydata/projectData/BackBone/indexes/backbone_" + graphsize + "_" + degree + "_" + dimension + "/";
        } else {
            indexFolder = homefolder + "/mydata/projectData/BackBone/indexes/backbone_" + graphsize + "_" + degree + "_" + dimension + "_backup_" + p + "/";
        }
        this.level = getlevel();
        System.out.println(level + " levels in the " + indexFolder);
        readIndex();
    }

    private int getlevel() {
        int level = 0;
        File index_dir = new File(indexFolder);
        System.out.println(indexFolder);
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

    public void queryBetween(long src, long dest) {
        long start_time = System.currentTimeMillis();
        /**deal with the source node **/
        HashSet<Long> src_nodes = new HashSet<>();
        HashMap<Long, HighwayNode> temp_source_nodes = new HashMap<>(); // highway_id --> highway node object
        HighwayNode src_highway_node = new HighwayNode(src);
        temp_source_nodes.put(src, src_highway_node);

        /**deal with the dest node **/
        HashSet<Long> dest_nodes = new HashSet<>();
        HashMap<Long, HighwayNode> temp_destination_nodes = new HashMap<>(); // highway_id --> highway node object
        HighwayNode dest_highway_node = new HighwayNode(dest);
        temp_destination_nodes.put(dest, dest_highway_node);

        src_nodes.add(src);
        dest_nodes.add(dest);

        for (int i = 0; i <= this.level; i++) {
            HashSet<Long> temp_src_nodes = new HashSet<>(); //the highway of src found in this level
            HashSet<Long> temp_dest_nodes = new HashSet<>(); // the highway of dest found in thie level

            boolean found_highway_in_current_level_src = false;
            boolean found_highway_in_current_level_dest = false;

            for (long src_node_id : src_nodes) {
                HighwayNode src_node = temp_source_nodes.get(src_node_id);
                HashMap<Long, ArrayList<double[]>> index_src = readHighwaysByIndex(i, src_node_id);
                if (index_src != null) {
                    for (Map.Entry<Long, ArrayList<double[]>> src_to_highway : index_src.entrySet()) {
                        long next_highway_node_id = src_to_highway.getKey();
                        if (next_highway_node_id == dest) {
                            found_highway_in_current_level_src = true;
                        }
                        ArrayList<double[]> next_skylines = src_to_highway.getValue();

                        HighwayNode next_highway;
                        if (temp_source_nodes.containsKey(next_highway_node_id)) {
                            next_highway = temp_source_nodes.get(next_highway_node_id);
                        } else {
                            next_highway = new HighwayNode(src, next_highway_node_id);
                        }

                        //very time consuming
                        for (backbonePath src_bp : src_node.skylines) {
                            for (double[] costs : next_skylines) {
                                backbonePath next_bp = new backbonePath(next_highway_node_id, costs, src_bp);
                                next_highway.addToSkyline(next_bp, monitor);
                            }
                        }
                        temp_source_nodes.put(next_highway_node_id, next_highway);
                    }
                }


                if (index_src != null) {
                    temp_src_nodes.addAll(index_src.keySet());
                }
            }


            for (long dest_node_id : dest_nodes) {
                HighwayNode dest_node = temp_destination_nodes.get(dest_node_id);
                HashMap<Long, ArrayList<double[]>> index_dest = readHighwaysByIndex(i, dest_node_id);

                if (index_dest != null) {
                    for (Map.Entry<Long, ArrayList<double[]>> dest_to_highway : index_dest.entrySet()) {
                        long next_highway_node_id = dest_to_highway.getKey();
                        if (next_highway_node_id == src) {
                            found_highway_in_current_level_dest = true;
                        }
                        ArrayList<double[]> next_skylines = dest_to_highway.getValue();

                        HighwayNode next_highway;
                        if (temp_destination_nodes.containsKey(next_highway_node_id)) {
                            next_highway = temp_destination_nodes.get(next_highway_node_id);
                        } else {
                            next_highway = new HighwayNode(src, next_highway_node_id);
                        }

                        //very time consuming
                        for (backbonePath dest_bp : dest_node.skylines) {
                            for (double[] costs : next_skylines) {
                                backbonePath next_bp = new backbonePath(next_highway_node_id, costs, dest_bp);
                                next_highway.addToSkyline(next_bp, monitor);
                            }
                        }
                        temp_destination_nodes.put(next_highway_node_id, next_highway);
                    }
                }


                if (index_dest != null) {
                    temp_dest_nodes.addAll(index_dest.keySet());
                }
            }

            src_nodes.addAll(temp_src_nodes);
            dest_nodes.addAll(temp_dest_nodes);

            System.out.println("level:" + i + " src_found_in_current_level:" + (temp_src_nodes == null ? 0 : temp_src_nodes.size()) + "    overall src:" + src_nodes.size() + "   " + monitor.callAddToSkyline);
            System.out.println("level:" + i + " des_found_in_current_level:" + (temp_src_nodes == null ? 0 : temp_dest_nodes.size()) + "    overall dest:" + dest_nodes.size() + "   " + monitor.callAddToSkyline);
            System.out.println(found_highway_in_current_level_src + "  " + found_highway_in_current_level_dest + "  " + (found_highway_in_current_level_src && found_highway_in_current_level_dest));
        }

        System.out.println((System.currentTimeMillis() - start_time) / 1000.0);

        long number_skyline_src = 0;
        long number_skyline_dest = 0;

        for (Map.Entry<Long, HighwayNode> src_e : temp_source_nodes.entrySet()) {
            number_skyline_src += src_e.getValue().skylines.size();
        }

        for (Map.Entry<Long, HighwayNode> dest_e : temp_destination_nodes.entrySet()) {
            number_skyline_dest += dest_e.getValue().skylines.size();
        }

        System.out.println(number_skyline_src + "/" + temp_source_nodes.size() + "=" + (number_skyline_src * 1.0 / temp_source_nodes.size()));
        System.out.println(number_skyline_dest + "/" + temp_destination_nodes.size() + "=" + (number_skyline_dest * 1.0 / temp_destination_nodes.size()));
    }

    private HashMap<Long, ArrayList<double[]>> readHighways(int level, long src) {
        String index_file = this.indexFolder + "level" + level + "/" + src + ".idx";
        System.out.println(index_file);
        if (!new File(index_file).exists()) {
            return null;
        }

        HashMap<Long, ArrayList<double[]>> tmpStoreNodes = new HashMap();
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(index_file));
            String line = "";
            while ((line = reader.readLine()) != null) {
                String[] informs = line.split(" ");
                long nodeid = Long.parseLong(informs[0]);
                double[] costs = new double[3];
                costs[0] = Double.parseDouble(informs[1]);
                costs[1] = Double.parseDouble(informs[2]);
                costs[2] = Double.parseDouble(informs[3]);

                ArrayList<double[]> skylines;
                if (tmpStoreNodes.containsKey(nodeid)) {
                    skylines = tmpStoreNodes.get(nodeid);
                } else {
                    skylines = new ArrayList<>();
                }
                skylines.add(costs);
                tmpStoreNodes.put(nodeid, skylines);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (tmpStoreNodes.size() != 0) {
            return tmpStoreNodes;
        } else {
            return null;
        }
    }


    private HashMap<Long, ArrayList<double[]>> readHighwaysByIndex(int level, long node_id) {
        HashMap<Long, HashMap<Long, ArrayList<double[]>>> level_index = this.index.get(level);
        if (!level_index.containsKey(node_id)) {
            return null;
        } else {
            HashMap<Long, ArrayList<double[]>> tmpStoreNodes = level_index.get(node_id);
            if (tmpStoreNodes.size() != 0) {
                return tmpStoreNodes;
            } else {
                return null;
            }
        }
    }

    public void readIndex() {
        this.index.clear();

        for (int level = 0; level <= this.level; level++) {
            File index_level_dir = new File(this.indexFolder + "/level" + level);
            System.out.println(index_level_dir);
            HashMap<Long, HashMap<Long, ArrayList<double[]>>> level_index = new HashMap<>();

            for (File f : index_level_dir.listFiles(new levelIndexFileNameFilter())) {
                BufferedReader reader;
                try {
                    long node_id = Long.parseLong(f.getName().substring(0, f.getName().indexOf(".idx")));
                    HashMap<Long, ArrayList<double[]>> tmpStoreNodes = new HashMap<>();

                    reader = new BufferedReader(new FileReader(f));
                    String line = "";
                    while ((line = reader.readLine()) != null) {
                        String[] informs = line.split(" ");
                        long highway_id = Long.parseLong(informs[0]);
                        double[] costs = new double[3];
                        costs[0] = Double.parseDouble(informs[1]);
                        costs[1] = Double.parseDouble(informs[2]);
                        costs[2] = Double.parseDouble(informs[3]);


                        ArrayList<double[]> skylines;
                        if (tmpStoreNodes.containsKey(highway_id)) {
                            skylines = tmpStoreNodes.get(highway_id);
                        } else {
                            skylines = new ArrayList<>();
                        }
                        skylines.add(costs);
                        tmpStoreNodes.put(highway_id, skylines);
                    }

                    level_index.put(node_id, tmpStoreNodes);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            this.index.add(level_index);

        }


    }

    public static class levelFileNameFilter implements FilenameFilter {
        @Override
        public boolean accept(File dir, String name) {
            return name.toLowerCase().startsWith("level");
        }
    }

    private class levelIndexFileNameFilter implements FilenameFilter {
        @Override
        public boolean accept(File dir, String name) {
            return name.toLowerCase().endsWith(".idx");
        }
    }
}
