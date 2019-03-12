package Query;

import Neo4jTools.Neo4jDB;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.Relationship;

import java.util.ArrayList;
import java.util.Collections;

public class backbonePath {
    private ArrayList<String> propertiesName;
    long source;
    long destination;
    double[] costs;

    ArrayList<Long> highwayList = new ArrayList<>();

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
}
