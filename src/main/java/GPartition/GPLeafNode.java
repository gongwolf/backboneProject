package GPartition;

public class GPLeafNode extends GPNode implements TreeNode {

    public GPLeafNode(GPTree rt) {
        super(rt);
    }

    @Override
    public void insert(Graph g) throws CloneNotSupportedException {
        this.sub_g = (Graph) g.clone();
    }

    @Override
    public void print(int printlevel, boolean showBorderInfo, boolean showMatrixInfo) {
        if (this.level <= printlevel) {
            String sb = "  ";
            for (int i = 0; i < this.level; i++) {
                sb += "  ";
            }

            double borderRatio = 1.0 * sub_g.borderNumber / sub_g.number_of_nodes;
            System.out.printf(sb + "level " + this.level + " " + getClass().getName() + " " + this.sub_g.number_of_nodes + " " + this.sub_g.number_of_edges + " [borderNodes]" + this.sub_g.borderNumber + " " + " %.2f \n", borderRatio);

            if (showBorderInfo) {
                for (PNode p : this.sub_g.gp_metis_formation.keySet()) {
                    System.out.println(sb + " " + p.current_id + " " + (p.neo4j_id + 1) + " " + (p.isBorder ? "true" : ""));
                }
            }
        }
    }
}
