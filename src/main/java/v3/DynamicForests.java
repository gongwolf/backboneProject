package v3;

import java.util.ArrayList;
import java.util.HashMap;

public class DynamicForests {
    HashMap<Integer,SpanningForsts> dforests;

    public DynamicForests(){
        dforests = new HashMap<>();
    }

    public void createBase(SpanningTree sptree_base) {
        int initLevel = 1;
        SpanningForsts sp = new SpanningForsts(initLevel);
        sp.trees.add(sptree_base);
        dforests.put(initLevel,sp);
    }

}
