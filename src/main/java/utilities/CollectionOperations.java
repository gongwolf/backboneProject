package utilities;

import org.checkerframework.checker.units.qual.K;
import v5LinkedList.clusterversion.NodeCoefficient;

import java.util.*;

public class CollectionOperations {

    public static HashMap<Long, NodeCoefficient> sortHashMapByValue(HashMap<Long, NodeCoefficient> source_map) {
        // Create a list from elements of HashMap
        List<Map.Entry<Long, NodeCoefficient>> list = new LinkedList<>(source_map.entrySet());

        // Sort the list
        try {
            Collections.sort(list, new Comparator<Map.Entry<Long, NodeCoefficient>>() {
                @Override
                public int compare(Map.Entry<Long, NodeCoefficient> o1, Map.Entry<Long, NodeCoefficient> o2) {
                    return (int) (o1.getValue().coefficient - o2.getValue().coefficient);
                }
            });
        }catch (IllegalArgumentException e){
            for(Map.Entry<Long, NodeCoefficient> aa:list) {
                System.out.println(aa.getValue());
            }
            e.printStackTrace();
            System.exit(0);
        }

        // put data from sorted list to hashmap
        HashMap<Long, NodeCoefficient> sorted_temp = new LinkedHashMap<>();
        for (Map.Entry<Long, NodeCoefficient> aa : list) {
            sorted_temp.put(aa.getKey(), aa.getValue());
        }
        return sorted_temp;
    }


}
