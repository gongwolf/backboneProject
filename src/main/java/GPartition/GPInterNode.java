package GPartition;

public class GPInterNode extends GPNode implements TreeNode {
//    public InterEntry entries[];// array of entries in the directory

    public GPNode son_ptrs[]; // the pointer point to the children
    public boolean is_leaf_son[]; // indicate whether each son is a leaf node.

    /***Information store in the node**/

    public GPInterNode(GPTree rt) {
        super(rt);
        son_ptrs = new GPNode[this.fan];
        is_leaf_son = new boolean[this.fan];
//        entries = new InterEntry[this.fan];
    }

    public void insert(Graph g) throws CloneNotSupportedException {
        this.sub_g = (Graph) g.clone();
        if (g.number_of_nodes >= this.vertex_in_leaf) {
            Graph[] sub_graphs = g.split(fan);
//            for (int i = 0; i < fan; i++) {
//                Graph sub_graph = sub_graphs[i];
//                GPNode node = new GPNode(this.my_tree);
//                node.level = this.level + 1;
//                son_ptrs[i] = node;
//
//                if (sub_graph.number_of_nodes <= this.vertex_in_leaf) {
//                    is_leaf_son[i] = true;
//                    ((GPLeafNode) node).insert(sub_graph);
//                } else {
//                    ((GPInterNode) node).insert(sub_graph);
//                }
//            }
        }
    }
}
