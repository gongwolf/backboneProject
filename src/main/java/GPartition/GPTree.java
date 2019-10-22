package GPartition;

import Neo4jTools.Neo4jDB;

public class GPTree {
    int fan;
    int threshold_vertex_in_leaf;
    public GPNode root_ptr;
    public boolean is_leaf_node;
    public Neo4jDB neo4jdb;


    public GPTree(int fan, int threshold_vertex_in_leaf) {
        root_ptr = null;
        this.fan = fan;
        this.threshold_vertex_in_leaf = threshold_vertex_in_leaf;
    }

    public void builtTree(Graph g) throws CloneNotSupportedException {
        this.neo4jdb = new Neo4jDB(g.graphsize + "_" + g.degree + "_" + g.dimension + "_Level0");
        this.neo4jdb.startDB(false);
        System.out.println(neo4jdb.DB_PATH);
        System.out.println(neo4jdb.graphDB);
        System.out.println(neo4jdb.getNumberofEdges());
        g.my_tree = this;

        //if the graph is smaller that threshold of the leaf, the tree point to a leaf node directly
        if (g.number_of_nodes <= threshold_vertex_in_leaf) {
            is_leaf_node = true;
            root_ptr = new GPLeafNode(this);
            root_ptr.sub_g = (Graph) g.clone();
            root_ptr.level = 0;
        } else {
//            System.out.println("The root is point to an internal tree node ");
            root_ptr = new GPInterNode(this);
            GPInterNode node = (GPInterNode) root_ptr;
            root_ptr.level = 0;
            node.insert(g);
        }

        neo4jdb.closeDB();

    }

    public void printTree(int printlevel) {
        if (this.is_leaf_node) {
            ((TreeNode) root_ptr).print(printlevel);
        } else {
            ((TreeNode) root_ptr).print(printlevel);
        }
    }
}
