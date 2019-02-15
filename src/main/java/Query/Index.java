package Query;

import java.io.*;
import java.util.ArrayList;
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

    public Index(int graphsize, int dimension, int degree) {
        this.graphsize = graphsize;
        this.degree = degree;
        this.dimension = dimension;

        index_folder = "/home/gqxwolf/mydata/projectData/BackBone/indexes/backbone_" + graphsize + "_" + degree + "_" + dimension;
        nth_folder = index_folder + "/nodeToHighway_index";

        readIndexFromDisk();
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

        for(File idx_file:index_folder_at_level_dir.listFiles(new idxFileNameFilter())){
            String filename = idx_file.getName();
            long highway_node = Long.parseLong(filename.substring(0,filename.lastIndexOf(".idx")));

            try (BufferedReader br = new BufferedReader(new FileReader(idx_file))) {
                String line;
                while ((line = br.readLine()) != null) {
                    System.out.println(line);

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


        String nodes_to_highway_index_file_path = index_folder+"/nodeToHighway_index/source_to_highway_index_level"+level+".idx";

        File nodes_to_highway_index_file = new File(nodes_to_highway_index_file_path);

        Hashtable<Long, ArrayList<Long>> nodesToHighWay=new Hashtable<>();

        try (BufferedReader br = new BufferedReader(new FileReader(nodes_to_highway_index_file))) {
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);

                long source_node_id = Long.parseLong(line.split(":")[0]);
                String highway_nodes = line.split(":")[1];

                for(String highway_str :highway_nodes.split(" ")){
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


    public static void main(String args[]) {
        Index i = new Index(1000, 3, 4);
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
