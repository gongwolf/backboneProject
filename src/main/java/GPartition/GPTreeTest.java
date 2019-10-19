package GPartition;

import java.util.HashMap;
import java.util.Map;

public class GPTreeTest {
    public static void main(String args[]) throws CloneNotSupportedException {
        GPTree tree = new GPTree(4, 30);

        String home_folder = System.getProperty("user.home");
        String graph_info_folder = home_folder + "/mydata/projectData/BackBone/testRandomGraph_40000_4_3/data";
        Graph init_graph = new Graph(graph_info_folder);
        tree.builtTree(init_graph);
        tree.printTree(4);

//        GPNode node;
//        if (tree.is_leaf_node) {
//            node = (tree.root_ptr);
//        } else {
//
//
//            System.out.println(tree.is_leaf_node+" "+((GPInterNode)tree.root_ptr).sub_g.number_of_nodes + " " + ((GPInterNode)tree.root_ptr).sub_g.borderNumber);
//
//            node = ((GPInterNode) tree.root_ptr).son_ptrs[0];
//            System.out.println(((GPInterNode) node).is_leaf_son[0] + "  " + node.sub_g.number_of_nodes + " " + node.sub_g.borderNumber);
//
//            while (!((GPInterNode) node).is_leaf_son[0]) {
//                node = ((GPInterNode) node).son_ptrs[0];
//                System.out.println(((GPInterNode) node).is_leaf_son[0] + "  " + node.sub_g.number_of_nodes + " " + node.sub_g.borderNumber);
//            }
//
//            GPLeafNode leafnode = (GPLeafNode) ((GPInterNode) node).son_ptrs[0];
//            System.out.println(leafnode.sub_g.number_of_nodes + " " + leafnode.sub_g.number_of_edges + " " + leafnode.sub_g.borderNumber);
//            leafnode = (GPLeafNode) ((GPInterNode) node).son_ptrs[1];
//            System.out.println(leafnode.sub_g.number_of_nodes + " " + leafnode.sub_g.number_of_edges + " " + leafnode.sub_g.borderNumber);
//            leafnode = (GPLeafNode) ((GPInterNode) node).son_ptrs[2];
//            System.out.println(leafnode.sub_g.number_of_nodes + " " + leafnode.sub_g.number_of_edges + " " + leafnode.sub_g.borderNumber);
//            leafnode = (GPLeafNode) ((GPInterNode) node).son_ptrs[3];
//            System.out.println(leafnode.sub_g.number_of_nodes + " " + leafnode.sub_g.number_of_edges + " " + leafnode.sub_g.borderNumber);
//
//
//        }

    }
}
