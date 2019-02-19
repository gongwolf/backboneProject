package Query;

public class backbonePath {
    long source;
    long destination;
    double[] costs;

    public backbonePath(long node_id) {
        this.source = node_id;
        this.destination = node_id;
        double[] costs = new double[3];
        costs[0]=costs[1]=costs[2]=0;
    }

    public backbonePath(long s_id, long h_node, double[] costs) {
        this.source = s_id;
    }
}
