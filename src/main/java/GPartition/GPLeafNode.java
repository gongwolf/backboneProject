package GPartition;

public class GPLeafNode extends GPNode implements TreeNode {

    public GPLeafNode(GPTree rt) {
        super(rt);
    }

    @Override
    public void insert(Graph g) throws CloneNotSupportedException {
        this.sub_g = (Graph) g.clone();
    }
}
