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
    public void print(int printlevel) {
        if (this.level <= printlevel) {
            String sb = " ";
            for (int i =0; i < this.level; i++) {
                sb += sb;
            }

            double borderRatio = 1.0 * sub_g.borderNumber / sub_g.number_of_nodes;
            System.out.printf(sb + "level " + this.level + " " + getClass().getName() + " " + this.sub_g.number_of_nodes + " " + this.sub_g.number_of_edges + " " + this.sub_g.borderNumber + " " + " %.2f \n", borderRatio);
        }
    }
}
