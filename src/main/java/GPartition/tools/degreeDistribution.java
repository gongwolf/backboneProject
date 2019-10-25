package GPartition.tools;

import javafx.util.Pair;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class degreeDistribution {
    String home_folder = System.getProperty("user.home");

    public static void main(String args[]) {
        degreeDistribution d = new degreeDistribution();
//        String edges_file = d.home_folder + "/mydata/projectData/BackBone/testRandomGraph_10000_2_3/data/SegInfo.txt";
//        String node_file = d.home_folder + "/mydata/projectData/BackBone/testRandomGraph_10000_2_3/data/NodeInfo.txt";

//        String edges_file = d.home_folder + "/mydata/projectData/BackBone/col_USA/SegInfo.txt";
//        String node_file = d.home_folder + "/mydata/projectData/BackBone/col_USA/NodeInfo.txt";


        String edges_file = d.home_folder + "/mydata/projectData/BackBone/busline_10000_0.0036/data/SegInfo.txt";
        String node_file = d.home_folder + "/mydata/projectData/BackBone/busline_10000_0.0036/data/NodeInfo.txt";


        d.calculation(node_file, edges_file);
    }

    private void calculation(String node_file, String edges_file) {
        HashMap<Long, Integer> nodes = new HashMap<>();
        try {
            BufferedReader br = new BufferedReader(new FileReader(node_file));
            String line = null;
            while ((line = br.readLine()) != null) {
                String[] attrs = line.split(" ");
                nodes.put(Long.valueOf(attrs[0]), 0);
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        HashSet<Pair<Long, Long>> edges = new HashSet<>();
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(edges_file));
            String line = null;
            while ((line = br.readLine()) != null) {
//                System.out.println(line);
                long sid = Integer.parseInt(line.split(" ")[0]);
                long did = Integer.parseInt(line.split(" ")[1]);
                Pair<Long, Long> relations = new Pair<>(sid, did);

                //Treat the graph as an in-directional graph
                if (!existedEdges(relations, edges)) {
                    edges.add(relations);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        long counter = 0;
        HashMap<Long, Long> degree_node_mapping = new HashMap<>();
        for (Map.Entry<Long, Integer> n : nodes.entrySet()) {
            if (counter % 10000 == 0) {
                System.out.println(counter + "  ..........................................");
            }
            long degree = getDegre(n.getKey(), edges);
//            if (degree != 0) {
//                System.out.println(n.getKey() + "   " + degree);
//            }


            counter++;

            if (degree_node_mapping.containsKey(degree)) {
                degree_node_mapping.put(degree, degree_node_mapping.get(degree) + 1);
            }else {
                degree_node_mapping.put(degree,1L);
            }
        }

        for(Map.Entry<Long, Long> e:degree_node_mapping.entrySet()){
            System.out.println(e.getKey()+"    "+e.getValue());
        }


    }

    private long getDegre(Long nodeid, HashSet<Pair<Long, Long>> edges) {
        List<Pair> list = new ArrayList<>(edges);
        return list.stream().filter(k -> ((long)k.getKey() == nodeid || (long)k.getValue() == nodeid)).count();
    }

    private boolean existedEdges(Pair<Long, Long> relations, HashSet<Pair<Long, Long>> edges) {

        long sid = relations.getKey();
        long did = relations.getValue();
        Pair<Long, Long> reverse_rel = new Pair<>(did, sid);
        return edges.contains(reverse_rel) || edges.contains(relations);

    }
}
