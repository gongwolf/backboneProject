package Query;

import java.io.*;
import java.util.*;

public class BackBoneIndex {
    private ArrayList<HashMap<Long, HashMap<Long, ArrayList<double[]>>>> index = new ArrayList();  //level --> <source --->{ highway id ==> <skyline paths > }  >
    private HashMap<Long, HashMap<Long, ArrayList<double[]>>> flat_index = new HashMap<>();  //source --->{ highway id ==> <skyline paths > }
    private final String index_folder;
    public int total_level;
    private int dimension = 3;

    public BackBoneIndex(String index_files_folder) {
        this.index_folder = index_files_folder;
        System.out.println("Read the index from the folder ------>>>>  " + index_files_folder);
        readIndexFromDisk();
    }

    private void readIndexFromDisk() {
        this.total_level = getlevel();
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


    public void transIndexToFlat(boolean writeToDisk) {
        this.flat_index.clear();

        for (HashMap<Long, HashMap<Long, ArrayList<double[]>>> level_index : this.index) {
            for (Map.Entry<Long, HashMap<Long, ArrayList<double[]>>> source_to_highway_idx : level_index.entrySet()) {
                long source_node_id = source_to_highway_idx.getKey();
                HashMap<Long, ArrayList<double[]>> highway_skyline_info = source_to_highway_idx.getValue();
                for (Map.Entry<Long, ArrayList<double[]>> e : highway_skyline_info.entrySet()) {
                    long highway_node_id = e.getKey();
                    ArrayList<double[]> skyline_costs = e.getValue();
                    for (double[] costs : skyline_costs) {
                        addElementToFlatIndex(source_node_id, highway_node_id, costs);
                    }
                }

            }
        }

        System.out.println("Finish the transfer from layer_index to flat_index");
    }

    private void addElementToFlatIndex(long source_node_id, long highway_node_id, double[] costs) {
        HashMap<Long, ArrayList<double[]>> highway_skyline_info = this.flat_index.get(source_node_id);
        if (highway_skyline_info == null) {
            highway_skyline_info = new HashMap<>();
        }

        ArrayList<double[]> skyline_paths = highway_skyline_info.get(highway_node_id);
        if (skyline_paths == null) {
            skyline_paths = new ArrayList<>();
        }

        boolean added_skyline_succ = addtoSkyline(skyline_paths, costs);

        if (added_skyline_succ) {
            highway_skyline_info.put(highway_node_id, skyline_paths);
            this.flat_index.put(source_node_id, highway_skyline_info);
        }

    }

    private boolean addtoSkyline(ArrayList<double[]> skyline_paths, double[] costs) {
        int i = 0;

        if (skyline_paths.isEmpty()) {
            skyline_paths.add(costs);
            return true;
        } else {
            boolean can_insert_np = true;
            for (; i < skyline_paths.size(); ) {
                if (checkDominated(skyline_paths.get(i), costs)) {
                    can_insert_np = false;
                    break;
                } else {
                    if (checkDominated(costs, skyline_paths.get(i))) {
                        skyline_paths.remove(i);
                    } else {
                        i++;
                    }
                }
            }
            if (can_insert_np) {
                skyline_paths.add(costs);
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

    public ArrayList<backbonePath> getNextHighwaysFlat(backbonePath p) {
        ArrayList<backbonePath> result = new ArrayList<>();
        HashMap<Long, ArrayList<double[]>> highway_skyline_info = flat_index.get(p.destination);
        if (highway_skyline_info != null) {
            for (Map.Entry<Long, ArrayList<double[]>> highways : highway_skyline_info.entrySet()) {
                long h_node = highways.getKey();
                for (double[] c : highways.getValue()) {
                    backbonePath new_bp = new backbonePath(h_node, c, p);
                    result.add(new_bp);
                }
            }
        }
        return result;
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
