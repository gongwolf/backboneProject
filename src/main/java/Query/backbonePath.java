package Query;

import Baseline.path;

import java.util.ArrayList;
import java.util.Collections;

public class backbonePath {
    boolean hasCycle;
    long source;
    long destination;
    public double[] costs;
    ArrayList<Long> highwayList = new ArrayList<>();
    private ArrayList<String> propertiesName;

    public backbonePath(long node_id) {
        this.source = node_id;
        this.destination = node_id;
        costs = new double[3];
        costs[0] = costs[1] = costs[2] = 0;
        this.highwayList.add(node_id);
    }

    public backbonePath(long h_node, double[] costs, backbonePath old_path) {
        this.source = old_path.source;
        this.destination = h_node;

        this.highwayList.clear();
        this.highwayList.addAll(old_path.highwayList);

        if (highwayList.contains(h_node)) {
            this.hasCycle = true;
        }

        this.highwayList.add(h_node);

        calculatedCosts(costs, old_path.costs);
    }

    public backbonePath(backbonePath s_t_h_bpath, backbonePath d_t_h_bpath) {
        this.source = s_t_h_bpath.source;
        this.destination = d_t_h_bpath.source;

        this.highwayList.clear();
        this.highwayList.addAll(s_t_h_bpath.highwayList);

        ArrayList<Long> reversed_highway = new ArrayList<>(d_t_h_bpath.highwayList);
        Collections.reverse(reversed_highway);

        this.highwayList.remove(this.highwayList.size() - 1);

        this.highwayList.addAll(reversed_highway);

        costs = new double[3];
        calculatedCosts(s_t_h_bpath.costs, d_t_h_bpath.costs);

//        System.out.println(s_t_h_bpath + "\n" + d_t_h_bpath);

    }

    public backbonePath(backbonePath s_t_h_bpath, backbonePath d_t_h_bpath, double[] costsInHighestLevel) {
        this.source = s_t_h_bpath.source;
        this.destination = d_t_h_bpath.source;

        this.highwayList.clear();
        this.highwayList.addAll(s_t_h_bpath.highwayList);

        ArrayList<Long> reversed_highway = new ArrayList<>(d_t_h_bpath.highwayList);
        Collections.reverse(reversed_highway);

        this.highwayList.addAll(reversed_highway);

        costs = new double[3];
        calculatedCosts(s_t_h_bpath.costs, d_t_h_bpath.costs);
        calculatedCosts(this.costs, costsInHighestLevel);
//        System.out.println(s_t_h_bpath + "\n ["+costsInHighestLevel[0]+" "+costsInHighestLevel[1]+" "+costsInHighestLevel[2]+"]\n " + d_t_h_bpath);


    }

    //the backbone paths in the highest level
    public backbonePath(long sid, long did, double[] costs) {
        this.source = sid;
        this.destination = did;
        this.costs = new double[3];
        System.arraycopy(costs, 0, this.costs, 0, this.costs.length);
    }

    public backbonePath(backbonePath source_bp, path bbs_p, backbonePath dest_bp) {
        this.source = source_bp.source;
        this.destination = dest_bp.source;

        this.highwayList.clear();
        this.highwayList.addAll(source_bp.highwayList);

        this.highwayList.addAll(bbs_p.nodes);

        ArrayList<Long> reversed_highway = new ArrayList<>(dest_bp.highwayList);
        Collections.reverse(reversed_highway);
        this.highwayList.addAll(reversed_highway);

        calculatedCosts(source_bp.costs, bbs_p.costs, dest_bp.costs);
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(source + " >>> " + destination + " [" + costs[0] + "," + costs[1] + "," + costs[2] + "] " + highwayList);
        return sb.toString();
    }

    private void calculatedCosts(double[] new_costs, double[] old_costs) {
        this.costs = new double[3];
        for (int i = 0; i < this.costs.length; i++) {
            this.costs[i] = old_costs[i] + new_costs[i];
        }
    }

    private void calculatedCosts(double[] source_costs, double[] bbs_costs, double[] dest_costs) {
        this.costs = new double[3];
        for (int i = 0; i < this.costs.length; i++) {
            this.costs[i] = source_costs[i] + bbs_costs[i] + dest_costs[i];
        }
    }
}
