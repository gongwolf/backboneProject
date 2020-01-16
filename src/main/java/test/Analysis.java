package test;

import java.io.*;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Analysis {
    public static void main(String args[]) {
        Analysis.findNodeCoverage();
    }

    private static void findNodeCoverage() {
        int divider = 10000;
        HashSet<Long> covered_node = new HashSet<>();
        String result_filename = "/home/gqxwolf/mydata/projectData/BackBone/busline_sub_graph_NY/results/backbone_3227_8222.txt";
        Pattern p = Pattern.compile("\\(.{1,4}\\)");
        try {
            BufferedReader b = new BufferedReader(new FileReader(result_filename));
            String readLine = "";
            while ((readLine = b.readLine()) != null) {
                String path = readLine.split(" ")[6];
                System.out.println(readLine);
                System.out.println(path);
                Matcher m = p.matcher(path);
                while(m.find()) {
                    String node_id_str = m.group(0);
                    String node_id = node_id_str.substring(1,node_id_str.length()-1);
                    System.out.print(node_id+",");
                    covered_node.add(Long.parseLong(node_id));
                }
                System.out.println();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(1.0*covered_node.size()/divider);
        System.out.println(covered_node.size());
        System.out.println("==============================================");
        for(long nd_id:covered_node){
            System.out.println(nd_id);
        }
    }
}
