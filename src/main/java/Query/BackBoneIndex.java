package Query;

import Index.Index;
import Query.Queue.myBackNode;

import java.io.*;
import java.util.*;

public class BackBoneIndex {
    public ArrayList<HashMap<Long, HashMap<Long, ArrayList<double[]>>>> index = new ArrayList();  //level --> <source --->{ highway id ==> <skyline paths > }  >
    private final String index_folder;
    public int total_level;
    private int dimension = 3;
    public IndexFlat flatindex;

    public BackBoneIndex(String index_files_folder) {
        this.index_folder = index_files_folder;
        this.flatindex = new IndexFlat();
        System.out.println("Read the index from the folder ------>>>>  " + index_files_folder);
        readIndexFromDisk();
    }

    private void readIndexFromDisk() {
        this.total_level = getlevel()+1;
        System.out.println("there are " + this.total_level + " level indexes");
        for (int i = 0; i < this.total_level; i++) {
            System.out.println("read the level " + i + "'s index ---->  " + i + "  to " + (i + 1));
            readIndexAtLevel(i);
        }
    }


    private int getlevel() {
        int level = 0;
        File index_dir = new File(this.index_folder);
        for (File f : index_dir.listFiles(new IndexQuery.levelFileNameFilter())) {
            String fname = f.getName();
            int c_level = Integer.parseInt(fname.substring(fname.indexOf("level") + 5));
            if (c_level > level) {
                level = c_level;
            }
        }
        return level;
    }


    private void readIndexAtLevel(int level) {
        String index_folder_at_level = index_folder + "/level" + level;

        File index_folder_at_level_dir = new File(index_folder_at_level);
        HashMap<Long, HashMap<Long, ArrayList<double[]>>> layer_index = new HashMap<>();

        for (File idx_file : index_folder_at_level_dir.listFiles(new idxFileNameFilter())) {

            String filename = idx_file.getName();
            long source_node = Long.parseLong(filename.substring(0, filename.lastIndexOf(".idx")));

            try (BufferedReader br = new BufferedReader(new FileReader(idx_file))) {

                String line;
                while ((line = br.readLine()) != null) {
                    long highway_node = Long.parseLong(line.split(" ")[0]);

                    double[] costs = new double[dimension];

                    costs[0] = Double.parseDouble(line.split(" ")[1]);
                    costs[1] = Double.parseDouble(line.split(" ")[2]);
                    costs[2] = Double.parseDouble(line.split(" ")[3]);

//                    costs[0] = new BigDecimal(String.valueOf(line.split(" ")[1])).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
//                    costs[1] = new BigDecimal(String.valueOf(line.split(" ")[2])).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
//                    costs[2] = new BigDecimal(String.valueOf(line.split(" ")[3])).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();

                    HashMap<Long, ArrayList<double[]>> source_to_highway_index;

                    if (!layer_index.containsKey(source_node)) {
                        ArrayList<double[]> skyline_index = new ArrayList<>();
                        skyline_index.add(costs);
                        source_to_highway_index = new HashMap<>();
                        source_to_highway_index.put(highway_node, skyline_index);
                    } else {
                        source_to_highway_index = layer_index.get(source_node);
                        if (!source_to_highway_index.containsKey(highway_node)) {
                            ArrayList<double[]> skyline_index = new ArrayList<>();
                            skyline_index.add(costs);
                            source_to_highway_index.put(highway_node, skyline_index);
                        } else {
                            ArrayList<double[]> skyline_index = source_to_highway_index.get(highway_node);
                            skyline_index.add(costs);
                            source_to_highway_index.put(highway_node, skyline_index);
                        }
                    }

                    layer_index.put(source_node, source_to_highway_index);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }


        }
        this.index.add(layer_index);
    }

    public HashSet<Long> getHighwayNodeAtLevel(int level, long source_node) {
        HashMap<Long, HashMap<Long, ArrayList<double[]>>> layer = index.get(level);
        HashMap<Long, ArrayList<double[]>> highwaynodes = layer.get(source_node);

        if (highwaynodes == null) {
            return null;
        } else {
            return new HashSet<>(highwaynodes.keySet());
        }
    }

    public ArrayList<double[]> readHighwaysInformation(long highway_node, int level, long source_node) {

        HashMap<Long, HashMap<Long, ArrayList<double[]>>> layer = index.get(level);
        HashMap<Long, ArrayList<double[]>> highway_nodes_index = layer.get(source_node);

        if (highway_nodes_index == null) {
            return null;
        } else {
            ArrayList<double[]> skyline_costs = highway_nodes_index.get(highway_node);
            return skyline_costs;
        }
    }


    public void buildFlatIndex() {
        flatindex.transIndexToFlat(true, index);
    }

    public void buildHighestFlatIndex(HashSet<Long> node_list) {
        this.flatindex.buildHighestFlatIndex(node_list);

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
