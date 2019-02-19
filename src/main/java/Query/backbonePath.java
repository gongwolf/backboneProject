package Query;

import java.util.ArrayList;

public class backbonePath {
    long source;
    long destination;
    double[] costs;

    ArrayList<Long> highwayList = new ArrayList<>();

    public backbonePath(long node_id) {
        this.source = node_id;
        this.destination = node_id;
        double[] costs = new double[3];
        costs[0] = costs[1] = costs[2] = 0;
        this.highwayList.add(node_id);
    }

    public backbonePath(long source_node, long h_node, double[] costs, backbonePath old_path) {
        this.source = old_path.source;
        this.destination = h_node;

        this.highwayList.clear();
        this.highwayList.addAll(old_path.highwayList);
        this.highwayList.add(h_node);

        calculatedCosts(costs,old_path.costs);
    }

    private void calculatedCosts(double[] new_costs, double[] old_costs) {
        this.costs = new double[3];
        System.arraycopy(old_costs,0,this.costs,0,this.costs.length);
        for(int i = 0;i<this.costs.length;i++){
            this.costs[i]+=new_costs[i];
        }
    }
}
