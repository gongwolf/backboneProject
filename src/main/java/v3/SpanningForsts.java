package v3;

public class SpanningForsts {
    Bag<SpanningTree> trees=new Bag<>();
    int level;

    public SpanningForsts(int level){
        this.level = level;
    }


    public void addNewTrees(SpanningTree sptree){
        trees.add(sptree);
    }
}
