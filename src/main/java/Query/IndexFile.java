package Query;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Map;

public class IndexFile {

    public ArrayList<Hashtable<Long, Hashtable<Long, ArrayList<double[]>>>> index = new ArrayList();  //level --> <node id --->{ highway id ==> <skyline paths > }  >
    public ArrayList<Hashtable<Long, ArrayList<Long>>> nodesToHighway_index = new ArrayList();        //level --> <source_node_id --> list of the highway>
    private int total_level;
    private String index_folder;
    private long graphsize;
    private int dimension;
    private int degree;

    public static void main(String args[]) {
        IndexFile i = new IndexFile();
        i.test(1000, 3, 4);

    }


    public void test(long graphsize, int dimension, int degree) {
        this.index_folder = "/home/gqxwolf/mydata/projectData/BackBone/indexes/backbone_" + graphsize + "_" + degree + "_" + dimension;
        this.graphsize = graphsize;
        this.dimension = dimension;
        this.degree = degree;
        this.total_level = getlevel();


        for (int i = 0; i <= this.total_level; i++) {
            readIndexAtLevel(i);
        }


        for (Hashtable<Long, Hashtable<Long, ArrayList<double[]>>> layer_index : this.index) {
            for (Map.Entry<Long, Hashtable<Long, ArrayList<double[]>>> hs : layer_index.entrySet()) {
                long highway_node = hs.getKey();
                Hashtable<Long, ArrayList<double[]>> source_to_highway = hs.getValue();

                if (highway_node == 0) {
                    System.out.println("11111");
                    ArrayList<double[]> lists;
                    if ((lists = source_to_highway.get(380L)) != null) {
                        for (double[] c : lists) {
                            System.out.println(c[0] + " " + c[1] + " " + c[2]);
                        }
                    }
                }

            }

        }
        System.out.println();
    }


    private int getlevel() {
        int level = 0;
        File index_dir = new File(this.index_folder);
        System.out.println(this.index_folder);
        for (File f : index_dir.listFiles(new IndexAccuracy.levelFileNameFilter())) {
            String fname = f.getName();
            int c_level = Integer.parseInt(fname.substring(fname.indexOf("level") + 5, fname.length()));

            if (c_level > level) {
                level = c_level;
            }
        }
        return level;
    }


    private void readIndexAtLevel(int level) {
        String index_folder_at_level = index_folder + "/level" + level;

        File index_folder_at_level_dir = new File(index_folder_at_level);
        Hashtable<Long, Hashtable<Long, ArrayList<double[]>>> layer_index = new Hashtable<>();

        for (File idx_file : index_folder_at_level_dir.listFiles(new IndexAccuracy.idxFileNameFilter())) {
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

}
