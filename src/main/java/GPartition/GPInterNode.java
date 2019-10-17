package GPartition;

public class GPInterNode extends GPNode{
    public InterEntry entries[];// array of entries in the directory

    public GPInterNode(GPTree rt) {
        super(rt);
        entries = new InterEntry[this.fan];
    }

    public void insert(Graph g) {
    }
}
