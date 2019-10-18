package GPartition;

public class GPTree {
    int fan;
    int threshold_vertex_in_leaf;
    public GPNode root_ptr;
    public boolean is_leaf_node;


    public GPTree(int fan, int threshold_vertex_in_leaf) {
        root_ptr = null;
        this.fan = fan;
        this.threshold_vertex_in_leaf = threshold_vertex_in_leaf;
    }

    public void builtTree(Graph g) throws CloneNotSupportedException {
        //if the graph is smaller that threshold of the leaf, the tree point to a leaf node directly
        if (g.number_of_nodes <= threshold_vertex_in_leaf) {
            is_leaf_node = true;
            root_ptr = new GPLeafNode(this);
            root_ptr.sub_g = (Graph) g.clone();
            root_ptr.level = 0;
        } else {
            System.out.println("The root is point to an internal tree node ");
            root_ptr = new GPInterNode(this);
            GPInterNode node = (GPInterNode) root_ptr;
            root_ptr.level = 0;
            node.insert(g);
        }

    }
}
