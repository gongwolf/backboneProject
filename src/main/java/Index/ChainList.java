package Index;

import Neo4jTools.Neo4jDB;

import java.util.ArrayList;

public class ChainList {
    public int size;
    public int min_chain_length = Integer.MAX_VALUE;
    public ArrayList<Chain> min_chain;
    public ArrayList<Chain> chainlist;

    public ChainList() {
        chainlist = new ArrayList<>();
        min_chain = new ArrayList<>();
        size = 0;
    }

    public ChainList(ChainList chainList) {
        this.size = chainList.size;
        this.min_chain_length = chainList.min_chain_length;

        this.min_chain = new ArrayList<Chain>();
        this.min_chain.addAll(chainList.min_chain);

        this.chainlist = new ArrayList<Chain>();
        this.chainlist.addAll(chainList.chainlist);
    }


    public void add(Chain c) {
        if (chainlist.isEmpty() || c.length < min_chain_length) {
            min_chain_length = c.length;
            min_chain.clear();
            min_chain.add(new Chain(c));
        } else if (c.length == min_chain_length) {
            min_chain.add(new Chain(c));
        }

        if (!this.chainlist.contains(c)) {
            this.chainlist.add(c);
            size++;
        }
    }

    public void clear() {
        chainlist = new ArrayList<>();
        min_chain = new ArrayList<>();
        size = 0;
    }

    public ChainList findChainsByRelationShip(long startNodeId, long endNodeId) {
        ChainList resutlChains = new ChainList();
        for (Chain c : this.chainlist) {
            if (c.containsRelationShip(startNodeId, endNodeId)) {
                resutlChains.add(c);
            }
        }
        return resutlChains;
    }

    public boolean isCandidateChain(Chain c) {
        for (Chain oc : this.chainlist) {
            if (!c.isPartOfChain(oc)) {
//                System.out.println("=====================================================");
//                System.out.print(oc+"\n"+c);
//                System.out.println("=====================================================");
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Chain c : this.chainlist) {
            sb.append(c).append("\n");
        }
        String result = sb.toString();
//        result = result.substring(0,result.length()-2);
        return result;
    }

    public void addAll(ChainList rList) {
        for (Chain c : rList.chainlist) {
            this.add(c);
        }
    }

    public void addAllNotOver(ChainList rList) {
        ChainList tempList = new ChainList(this);

        for (Chain c : rList.chainlist) {
            boolean deleted = false;
            int j;

            for (j = 0; j < tempList.size; ) {
                //if any chain in current chainlist is the subset of chain c
                //It means not need to insert the new c
                //The proof the transitive is same as our first paper
                if (tempList.chainlist.get(j).isPartOfChain(c)) {
                    break;
                }

                //if chain c is one part of one of chain in current chainlist,
                //It means some chains need to be removed from old chainlist, and c need to add to the new chainlist
                if (c.isPartOfChain(tempList.chainlist.get(j))) {
                    tempList.remove(j);
                    deleted = true;
                }

                if (!deleted) {
                    j++;
                }
            }

            if (deleted || (!deleted && j == tempList.size)) {
                tempList.add(c);
            }
        }

        this.copyOf(tempList);
    }

    private void copyOf(ChainList source_list) {
        this.size = source_list.size;
        this.min_chain_length = source_list.min_chain_length;

        this.min_chain = new ArrayList<Chain>();
        this.min_chain.addAll(source_list.min_chain);

        this.chainlist = new ArrayList<Chain>();
        this.chainlist.addAll(source_list.chainlist);
    }

    private void remove(int index) {
        this.size--;
        this.chainlist.remove(index);
        updateMinChain();
    }

    private void updateMinChain() {
        int min = Integer.MAX_VALUE;
        for (int i = 0; i < size; i++) {
            Chain c = this.chainlist.get(i);
            if (c.length < min) {
                min_chain_length = c.length;
                min_chain.clear();
                min_chain.add(new Chain(c));
            } else if (c.length == min) {
                min_chain.add(new Chain(c));
            }
        }
    }

    public ChainComponents formChainComponent(Neo4jDB neo4j) {
        ChainComponents ccps = new ChainComponents(neo4j);
        for (Chain c : this.chainlist) {
            ccps.add(c);
        }
//        System.out.println(ccps);
//        System.out.println("------------");

        ccps.mergeComponents();

        return ccps;
    }
}
