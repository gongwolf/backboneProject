package v3;

import org.neo4j.graphdb.Relationship;

public class SpanningForests {
    Bag<SpanningTree> trees = new Bag<>();
    int level;

    public SpanningForests(int level) {
        this.level = level;
    }

    public SpanningTree findTree(Relationship r) {
        Bag.Node<SpanningTree> current_tree = trees.first;
        if (current_tree == null) {
            System.out.println("there is no spanning tree in level " + level);
        }
        while (current_tree != null) {
            if(current_tree.item.hasEdge(r)){
                System.out.println("find spanning tree whose contains the edge "+r);
            }
            current_tree = current_tree.next;
        }

        //find process until the last element return false
        return current_tree == null ? null : current_tree.item;
    }


    public void addNewTrees(SpanningTree sptree) {
        trees.add(sptree);
    }
}
