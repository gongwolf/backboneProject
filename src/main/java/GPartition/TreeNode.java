package GPartition;

public interface TreeNode {
    public abstract void insert(Graph g) throws CloneNotSupportedException;

    public void print(int printlevel, boolean showBorderInfo, boolean showMatrixInfo) ;
}
