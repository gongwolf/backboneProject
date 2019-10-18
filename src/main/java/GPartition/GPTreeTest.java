package GPartition;

public class GPTreeTest {
    public static void main(String args[]) throws CloneNotSupportedException {
        GPTree tree = new GPTree(4, 30);

        String home_folder = System.getProperty("user.home");
        String graph_info_folder = home_folder + "/mydata/projectData/BackBone/testRandomGraph_10000_4_3/data";
        Graph init_graph = new Graph(graph_info_folder);
        tree.builtTree(init_graph);
    }
}
