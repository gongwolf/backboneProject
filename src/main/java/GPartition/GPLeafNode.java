package GPartition;

public class GPLeafNode extends GPNode {
    Graph leaf_graph;

    public GPLeafNode(GPTree rt) {
        super(rt);
        this.leaf_graph = new Graph();

    }
}
