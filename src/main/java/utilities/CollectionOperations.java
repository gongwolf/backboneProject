package utilities;

import java.util.*;

public class CollectionOperations {

    public static <K, V extends Comparable<? super V>> HashMap<K, V> sortHashMapByValue(HashMap<K, V> source_map) {
        // Create a list from elements of HashMap
        List<Map.Entry<K, V>> list = new LinkedList<>(source_map.entrySet());

        // Sort the list
        Collections.sort(list, Comparator.comparing(Map.Entry::getValue));

        // put data from sorted list to hashmap
        HashMap<K, V> sorted_temp = new LinkedHashMap<>();
        for (Map.Entry<K, V> aa : list) {
            sorted_temp.put(aa.getKey(), aa.getValue());
        }
        return sorted_temp;
    }


}
