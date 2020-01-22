package test;

import Query.QueryProcess;
import Query.Queue.myBackNode;
import Query.backbonePath;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Analysis {

    public static void main(String args[]) {
        Analysis.test();
    }

    /**
     * Test the size of the flat index at the highest level
     */
    public static void test() {
//        QueryProcess query = new QueryProcess();
//        long running_time = System.currentTimeMillis();
//
//        long overall_size = 0;
//        for (long id : query.bbs.node_list) {
//            if (id != 8376) {
//                continue;
//            }
//
//            HashMap<Long, myBackNode> tmpResult = query.BBSQueryAtHighlevelGrpah(id);
//            int size = 0;
//            for (Map.Entry<Long, myBackNode> e : tmpResult.entrySet()) {
//                size += e.getValue().skypaths.size();
//                for (backbonePath bp : e.getValue().skypaths) {
//                    System.out.println(bp);
//                }
//            }
//            System.out.println(id + "    " + size);
//
//
//            overall_size += size;
//        }
//
//        System.out.println("Overall size :   " + overall_size + "  running in " + (System.currentTimeMillis() - running_time) + "  ms ");

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
                while (m.find()) {
                    String node_id_str = m.group(0);
                    String node_id = node_id_str.substring(1, node_id_str.length() - 1);
                    System.out.print(node_id + ",");
                    covered_node.add(Long.parseLong(node_id));
                }
                System.out.println();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(1.0 * covered_node.size() / divider);
        System.out.println(covered_node.size());
        System.out.println("==============================================");
        for (long nd_id : covered_node) {
            System.out.println(nd_id);
        }
    }
}
