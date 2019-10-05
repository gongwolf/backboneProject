package v4;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class Query {
    private int level;
    private String indexFolder;
    private int graphsize;
    private int degree;
    private int dimension;
    String homefolder = System.getProperty("user.home");
    private ArrayList<HashMap<Long, HashMap<Long, ArrayList<double[]>>>> index = new ArrayList<>(); // < level--> < node--> <highway_nodes, list<skyline paths>>>>

    public Query(int graphsize, int degree, int dimension) {
        this.graphsize = graphsize;
        this.degree = degree;
        this.dimension = dimension;
        indexFolder = homefolder + "/mydata/projectData/BackBone/indexes/backbone_" + graphsize + "_" + degree + "_" + dimension + "/";
        this.level = getlevel();
        System.out.println(level + " levels in the " + indexFolder);
        readIndex();
    }

    private int getlevel() {
        int level = 0;
        File index_dir = new File(indexFolder);
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
        HashSet<Long> src_nodes = new HashSet<>();
        src_nodes.add(src);
        for (int i = 0; i <= level; i++) {
            HashSet<Long> temp_src_nodes = new HashSet<>();

            for (long node_src : src_nodes) {
                HashMap<Long, ArrayList<double[]>> index_src = readHighwaysByIndex(i, node_src);
                if (index_src != null) {
                    for (long next_level_src_nodes : index_src.keySet()) {
                        temp_src_nodes.add(next_level_src_nodes);
                    }
                }
            }

            for(long new_src:temp_src_nodes){
                src_nodes.add(new_src);
            }

            System.out.println(temp_src_nodes == null ? 0 : temp_src_nodes.size()+"    "+src_nodes.size());

        }
//        System.out.println("==========================================================");
//        for (int i = 0; i <= level; i++) {
//            HashMap<Long, ArrayList<double[]>> index_src = readHighwaysByIndex(i, dest);
//            System.out.println(index_src == null ? 0 : index_src.size());
//        }
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
//                System.out.println(" ~~~~ |" + line + "|" + (line == null));

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


    public static void main(String args[]) {
        int graphesize = 2000;
        int degree = 4;
        int dimension = 3;

        Query q = new Query(graphesize, degree, dimension);
        q.queryBetween(1, graphesize - 1);
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
