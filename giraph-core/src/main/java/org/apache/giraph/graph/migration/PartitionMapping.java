package org.apache.giraph.graph.migration;

/**
 * Created by ravikant on 24/1/17.
 */
//public class PartitionMapping {
//}


//package org.apache.giraph.examples.binpacking;

//        import com.google.common.collect.ArrayListMultimap;
//        import com.google.common.collect.ListMultimap;
//        import com.google.common.collect.Multimaps;
//        import com.google.common.collect.Multiset;

import com.google.common.collect.Sets;

import java.util.*;

/**
 * Created by ravikant on 24/1/17.
 */
public class PartitionMapping {

  //TODO: apply brute force approach
  //TODO: return type should be map of worker to partition map
  //TODO: keep a set of available workers so that in case of scale out , we can use unused workers
  public static Map<Integer, Set<Integer>> computeVmMapping(Map<Integer, Set<Integer>> old_worker2partitionMap, List<Set<Integer>> new_worker2partitionMap, int num_workers) {

    Map<Integer, Set<Integer>> current_worker2partitionMap = new HashMap<>();

    //TODO: handle case when set difference is equal to the set length
    List<Set<Integer>> remaining_worker2partitionList = new ArrayList<>();

    Set<Integer> availableWorkerSet = new HashSet<>();

    for (int i = 1; i <= num_workers; i++) {

      availableWorkerSet.add(i);
    }

    //length can be different. Compare vm from second map with workers from first map
    //both cases are possible : scale out and scale in
    Iterator<Set<Integer>> new_entries = new_worker2partitionMap.iterator();

    boolean assigned = false;

    while (new_entries.hasNext()) {

      Set<Integer> new_entry = new_entries.next();

      //compare with each entry in old mappings

      int min_dist = Integer.MAX_VALUE;
      int worker_with_min_dist = -1;

      Iterator<Map.Entry<Integer, Set<Integer>>> old_entries = old_worker2partitionMap.entrySet().iterator();

      while (old_entries.hasNext()) {

        assigned = false;

        Map.Entry<Integer, Set<Integer>> old_entry = old_entries.next();

        //returns notin set2
        Sets.SetView<Integer> difference = Sets.difference(new_entry, old_entry.getValue());

//                difference.size()
        System.out.println("calc_diff " + old_entry.getKey() + ":" + old_entry.getValue() + ":" + new_entry + " " + difference.size());


        if (difference.size() == 0) {

          assigned = true;

          current_worker2partitionMap.put(old_entry.getKey(), new_entry);

          System.out.println("mapped:" + old_entry.getKey() + " " + new_entry);

          availableWorkerSet.remove(old_entry.getKey());
//                    new_entries.remove();
          old_entries.remove();


          break;
        }
        if (difference.size() < min_dist) {
          min_dist = difference.size();
          worker_with_min_dist = old_entry.getKey();
        }


      }

      if (assigned)
        continue;

      //when no match found map it later
      if (min_dist == new_entry.size()) {

        remaining_worker2partitionList.add(new_entry);

        System.out.println("evaluate later :" + new_entry);


      } else if (min_dist != Integer.MAX_VALUE) { // map to existing worker with maximum matching

        //assign the min distance
        current_worker2partitionMap.put(worker_with_min_dist, new_entry);

        System.out.println("mapped:" + worker_with_min_dist + " " + new_entry);
//                new_entries.remove();
        old_worker2partitionMap.remove(worker_with_min_dist);
        availableWorkerSet.remove(worker_with_min_dist);

      } else { //spawn a new worker
        //assign a worker from available worker
        int wid = availableWorkerSet.iterator().next();
        current_worker2partitionMap.put(wid, new_entry);
        availableWorkerSet.remove(wid);
        System.out.println("spawned new worker:" + wid + " " + new_entry);
      }
    }

    //If still active workers availble map the remaining workers to those else spawn a new worker

    Iterator<Set<Integer>> remaining_entries = remaining_worker2partitionList.iterator();

    while (remaining_entries.hasNext()) {

      Set<Integer> remaining_entry = remaining_entries.next();

      if (old_worker2partitionMap.size() != 0) {

        Map.Entry<Integer, Set<Integer>> old_entry = old_worker2partitionMap.entrySet().iterator().next();

        current_worker2partitionMap.put(old_entry.getKey(), remaining_entry);

        System.out.println("remaining mapped:" + old_entry.getKey() + " " + remaining_entry);

        availableWorkerSet.remove(old_entry.getKey());


      } else {//spawn a new worker
        int wid = availableWorkerSet.iterator().next();
        current_worker2partitionMap.put(wid, remaining_entry);
        availableWorkerSet.remove(wid);
        System.out.println("remaining spawned new worker:" + wid + " " + remaining_entry);
      }
    }


    //print solution
    Iterator<Map.Entry<Integer, Set<Integer>>> final_entries = current_worker2partitionMap.entrySet().iterator();

    System.out.println("Printitng solution");

    while (final_entries.hasNext()) {
      Map.Entry<Integer, Set<Integer>> final_entry = final_entries.next();
      System.out.println(final_entry.getKey() + " " + final_entry.getValue());
    }


    return current_worker2partitionMap;

  }


//  public static void main(String[] args) {
//
//    PartitionMapping r = new PartitionMapping();
//
//    Map<Integer, Set<Integer>> map1 = new HashMap<>();
//
//    map1.put(1, Sets.newHashSet(2, 3));
//    map1.put(2, Sets.newHashSet(4, 5));
//    map1.put(3, Sets.newHashSet(10));
//
//
//    ArrayList<Set<Integer>> map2 = new ArrayList<>();
//
//    map2.add(Sets.newHashSet(4, 5));
//    map2.add(Sets.newHashSet(7, 6));
//    map2.add(Sets.newHashSet(2));
//
//    r.computeVmMapping(map1, map2, 10);


//        Map<Integer,Integer> partition2workerMap= new HashMap<>();
//
//        partition2workerMap.put(1,1);
//        partition2workerMap.put(2,1);
//        partition2workerMap.put(3,1);
//        partition2workerMap.put(4,1);
//
//        partition2workerMap.put(5,2);
//        partition2workerMap.put(6,2);
//        partition2workerMap.put(7,2);
//        partition2workerMap.put(8,2);
//
//        Map<Integer,Set<Integer>> worker2partitionMap= new HashMap<>();
//
//        Map<Integer,Set<Integer>> worker2partitionMap2= new HashMap<>();
//
//        Iterator<Map.Entry<Integer, Integer>> entries = partition2workerMap.entrySet().iterator();
//        while (entries.hasNext()) {
//            Map.Entry<Integer, Integer> entry = entries.next();
//            System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
//            if(worker2partitionMap.containsKey(entry.getValue())){
//                worker2partitionMap.get(entry.getValue()).add(entry.getKey());
//                worker2partitionMap2.get(entry.getValue()).add(entry.getKey());
//            }
//            else{
//                Set<Integer> l= new HashSet<>();
//                l.add(entry.getKey());
//                worker2partitionMap.put(entry.getValue(),l);
//                worker2partitionMap2.put(entry.getValue(),l);
//            }
//        }
//
//
//
//
//        Iterator<Map.Entry<Integer, Set<Integer> >> e = worker2partitionMap.entrySet().iterator();
//        while(e.hasNext()){
//
//            Map.Entry<Integer, Set<Integer>> entry = e.next();
//            System.out.println("Key2 = " + entry.getKey() + ", Value2 = " + entry.getValue());
//
//
//        }

//
//        ArrayListMultimap<Integer, Integer> inverse = Multimaps.invertFrom(Multimaps.forMap(partition2workerMap),
//                ArrayListMultimap.<Integer,Integer>create());
//
//
//        Iterator entries = partition2workerMap.entrySet().iterator();
//
//        while (entries.hasNext()) {
////            Multiset.Entry thisEntry = (Multiset.Entry) entries.next();
////            Object key = thisEntry.getKey();
////            Object value = thisEntry.getValue();
////            // ...
//
//
//        }
//  }

}
