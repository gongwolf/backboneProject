package GPartition;

public class GPNode {

    Graph sub_g;
    public GPTree my_tree;

    int fan;
    int vertex_in_leaf;

    public GPNode(GPTree rt) {
        this.my_tree = rt;
        this.fan = rt.fan;
        this.vertex_in_leaf = rt.threshold_vertex_in_leaf;
    }

}
