package Query;

import Query.Queue.MyQueue;
import Query.Queue.myBackNode;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class IndexFlat {

    public HashMap<Long, HashMap<Long, ArrayList<double[]>>> flat_index = new HashMap<>();  //source --->{ highway id ==> <skyline paths > }
    public HashMap<Long, HashMap<Long, ArrayList<double[]>>> highest_index = new HashMap<>();
    private int overall_highest_index_size;


    public void transIndexToFlat(boolean writeToDisk, ArrayList<HashMap<Long, HashMap<Long, ArrayList<double[]>>>> index) {
        this.flat_index.clear();

        for (HashMap<Long, HashMap<Long, ArrayList<double[]>>> level_index : index) {
            for (Map.Entry<Long, HashMap<Long, ArrayList<double[]>>> source_to_highway_idx : level_index.entrySet()) {
                long source_node_id = source_to_highway_idx.getKey();
                HashMap<Long, ArrayList<double[]>> highway_skyline_info = source_to_highway_idx.getValue();
                for (Map.Entry<Long, ArrayList<double[]>> e : highway_skyline_info.entrySet()) {
                    long highway_node_id = e.getKey();
                    ArrayList<double[]> skyline_costs = e.getValue();
                    for (double[] costs : skyline_costs) {
                        addElementToIndex(source_node_id, highway_node_id, costs, this.flat_index);
                    }
                }
            }
        }
        System.out.println("Finish the transfer from layer_index to flat_index");
    }

    public void buildHighestFlatIndex(HashSet<Long> node_list) {
        System.out.println("There are " + node_list.size() + "  nodes .");
        long running_time = System.currentTimeMillis();
        this.overall_highest_index_size = 0;
        int overall_size = 0;
        for (long id : node_list) {
            System.out.print("Process the node ::::>>>>>>>>  " + id );
            long rt = System.currentTimeMillis();
            HashMap<Long, myBackNode> tmpResult = BBSQueryAtHighlevelGrpah(id, node_list);
            int size = 0;
            int highway_node_size = 0;
            for (Map.Entry<Long, myBackNode> e : tmpResult.entrySet()) {
                if (node_list.contains(e.getKey())) {
                    size += e.getValue().skypaths.size();
                    highway_node_size++;
                    for (backbonePath bp : e.getValue().skypaths) {
                        addElementToIndex(id, e.getKey(), bp.costs, this.highest_index);
                    }
                }
            }
            overall_size += size;
            System.out.println("  in " + (System.currentTimeMillis() - rt) + "  ms");
        }

        this.overall_highest_index_size = overall_size;
        System.out.println("Overall size :   " + overall_highest_index_size + "  running in " + (System.currentTimeMillis() - running_time) + "  ms   " + this.highest_index.size());
    }

    private void addElementToIndex(long source_node_id, long highway_node_id, double[] costs, HashMap<Long, HashMap<Long, ArrayList<double[]>>> index_structure) {
        HashMap<Long, ArrayList<double[]>> highway_skyline_info = index_structure.get(source_node_id);

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
            index_structure.put(source_node_id, highway_skyline_info);
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
//        for (int i = 0; i < costs.length; i++) {
//            if (costs[i] > estimatedCosts[i]) {
//                return false;
//            }
//        }
//        return true;

        int numberNotEqual = 0;
        for (int i = 0; i < costs.length; i++) {
            if (costs[i] > estimatedCosts[i]) {
                return false;
            } else if (costs[i] < estimatedCosts[i] && numberNotEqual == 0) {
                numberNotEqual++;
            }
        }

        if (numberNotEqual != 0) {
            return true;
        } else {
            return false;
        }
    }

    public HashMap<Long, myBackNode> BBSQueryAtHighlevelGrpah(long start_node, HashSet<Long> node_list) {
        HashMap<Long, myBackNode> tmpStoreNodes = new HashMap();
        myBackNode s_my_bnode = new myBackNode(start_node);
        MyQueue queue = new MyQueue();
        queue.add(s_my_bnode);
        tmpStoreNodes.put(s_my_bnode.id, s_my_bnode);

        while (!queue.isEmpty()) {
            myBackNode v = queue.pop();
            for (int i = 0; i < v.skypaths.size(); i++) {
                backbonePath p = v.skypaths.get(i);
                if (!p.expanded) {
                    p.expanded = true;
                    ArrayList<backbonePath> new_paths = getNextHighwaysFlat(p);
                    for (backbonePath n_bp : new_paths) {
                        myBackNode next_n;
                        if (tmpStoreNodes.containsKey(n_bp.destination)) {
                            next_n = tmpStoreNodes.get(n_bp.destination);
                        } else {
                            next_n = new myBackNode(n_bp);
                            tmpStoreNodes.put(next_n.id, next_n);
                        }

                        if (node_list.contains(next_n.id)) {
                            next_n.addtoSkyline(n_bp);
                        } else if (next_n.addtoSkyline(n_bp) && !next_n.inqueue) {
                            tmpStoreNodes.put(next_n.id, next_n);
                            queue.add(next_n);
                        }

                    }
                }
            }
        }
        return tmpStoreNodes;
    }

    public ArrayList<backbonePath> getNextHighwaysFlat(backbonePath p) {
        ArrayList<backbonePath> result = new ArrayList<>();
        HashMap<Long, ArrayList<double[]>> highway_skyline_info = flat_index.get(p.destination);
        if (highway_skyline_info != null) {
            for (Map.Entry<Long, ArrayList<double[]>> highways : highway_skyline_info.entrySet()) {
                long h_node = highways.getKey();
                if (p.destination != h_node) {
                    for (double[] c : highways.getValue()) {
                        backbonePath new_bp = new backbonePath(h_node, c, p);
                        result.add(new_bp);
                    }
                }
            }
        }
        return result;
    }

    public ArrayList<backbonePath> expand(backbonePath bp) {
        ArrayList<backbonePath> result = new ArrayList<>();
        HashMap<Long, ArrayList<double[]>> highway_infos = this.highest_index.get(bp.destination);
        if (highway_infos != null) {
            for (Map.Entry<Long, ArrayList<double[]>> e : highway_infos.entrySet()) {
                long h_id = e.getKey();
                for (double[] cost : e.getValue()) {
                    backbonePath new_p = new backbonePath(h_id, cost, bp);
                    result.add(new_p);
                }
            }
        }
        return result;
    }

    public void writeHighestFlatIndexToDisk(String idx_file_name) {
        FileWriter fileWriter = null;
        try {
            fileWriter = new FileWriter(idx_file_name);
            PrintWriter printWriter = new PrintWriter(fileWriter);
            for (Map.Entry<Long, HashMap<Long, ArrayList<double[]>>> source_element : this.highest_index.entrySet()) {
                long source_node_id = source_element.getKey();
                for (Map.Entry<Long, ArrayList<double[]>> shortcut_information : source_element.getValue().entrySet()) {
                    long highway_node_id = shortcut_information.getKey();
                    for (double[] costs : shortcut_information.getValue()) {
                        printWriter.println(source_node_id + " " + highway_node_id + " " + costs[0] + " " + costs[1] + " " + costs[2]);
                    }
                }
            }
            printWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void readHighestFlatIndexToDisk(String idx_file_name) {
        long running_time = System.currentTimeMillis();
        this.highest_index.clear();
        this.overall_highest_index_size = 0;
        BufferedReader br;
        try {
            br = new BufferedReader(new FileReader(idx_file_name));
            String line;
            while ((line = br.readLine()) != null) {
                String[] infos = line.split(" ");
                long source_id = Long.parseLong(infos[0]);
                long highway_id = Long.parseLong(infos[1]);

                double cost[] = new double[3];
                cost[0] = Double.parseDouble(infos[2]);
                cost[1] = Double.parseDouble(infos[3]);
                cost[2] = Double.parseDouble(infos[4]);
                addElementToIndex(source_id, highway_id, cost, this.highest_index);
            }
            br.close();

            for (Map.Entry<Long, HashMap<Long, ArrayList<double[]>>> source_list : this.highest_index.entrySet()) {
                for (Map.Entry<Long, ArrayList<double[]>> highway_list : source_list.getValue().entrySet()) {
                    overall_highest_index_size += highway_list.getValue().size();
                }
            }

            System.out.println("Overall size :   " + overall_highest_index_size + "  running in " + (System.currentTimeMillis() - running_time) + "  ms   " + this.highest_index.size());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
