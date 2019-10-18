package GPartition;

public class GPNode {

    Graph sub_g;
    public GPTree my_tree; // the root of the tree


    int fan; // the fanout of the node
    int vertex_in_leaf; // the threshold of the number of vertex in each leaf
    int level; //level of the node

    public GPNode(GPTree rt) {
        this.my_tree = rt;
        this.fan = rt.fan;
        this.vertex_in_leaf = rt.threshold_vertex_in_leaf;
    }

}
