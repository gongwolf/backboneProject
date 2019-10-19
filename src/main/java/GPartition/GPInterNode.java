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
//        System.out.println(" insert ::::::::::: "+g.number_of_nodes+"  "+g.number_of_edges+" "+g.gp_metis_formation.size());
//        this.sub_g = (Graph) g.clone();
        this.sub_g = (Graph) g.clone();
        if (g.number_of_nodes >= this.vertex_in_leaf) {
            Graph[] sub_graphs = this.sub_g.split(fan);
            for (int i = 0; i < fan; i++) {
                Graph sub_graph = sub_graphs[i];
                GPNode node;
                if (sub_graph.number_of_nodes <= this.vertex_in_leaf) {
                    is_leaf_son[i] = true;
                    node = new GPLeafNode(this.my_tree);
                    node.level = this.level + 1;
                    ((GPLeafNode) node).insert(sub_graph);
                    son_ptrs[i] = node;
//                    System.out.println("leaf node at " + sub_graph.level + " " + sub_graph.number_of_nodes + " " + sub_graph.number_of_edges + " " + sub_graph.borderNumber);
                } else {
                    node = new GPInterNode(this.my_tree);
                    node.level = this.level + 1;
                    ((GPInterNode) node).insert(sub_graph);
                    son_ptrs[i] = node;
                }
            }
        }
    }

    @Override
    public void print(int printlevel) {

        if (this.level <= printlevel) {
            String sb = " ";
            for (int i = 0; i < this.level; i++) {
                sb += sb;
            }
            double borderRatio = 1.0 * sub_g.borderNumber / sub_g.number_of_nodes;
            System.out.printf(sb + "level " + this.level + " " + getClass().getName() + " " + this.sub_g.number_of_nodes + " " + this.sub_g.number_of_edges + " " + this.sub_g.borderNumber + " %.2f \n", borderRatio);
            for (int i = 0; i < son_ptrs.length; i++) {
                ((TreeNode) son_ptrs[i]).print(printlevel);
            }
        }
    }
}
