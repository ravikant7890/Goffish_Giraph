package org.apache.giraph.graph.migration;

import java.util.*;


/**
 * Created by ravikant on 16/1/17.
 */
public class FirstFitDecreasing {

  //TODO: compute the capacity here
  //TODO: return Map<Integer,Set<Integer>>  as packing
  public static List<Set<Integer>> pack(ArrayList<MappingRow> partitions){

    float capacity=0;
    for (MappingRow partition : partitions) {

      if(partition.getComputeTime() > capacity)
        capacity=partition.getComputeTime();
    }


    Collections.sort(partitions, new Comparator<MappingRow>() {
      @Override
      public int compare(MappingRow t1, MappingRow t2) {
        return (int) (t2.getComputeTime()-t1.getComputeTime());
      }
    });


    for (MappingRow partition : partitions) {

      System.out.println(partition.getComputeTime());
    }


    List<Set<Integer>> bins = new ArrayList<>();
    List<Float> binSums = new ArrayList<>();
    for (MappingRow item : partitions) {

      boolean assigned=false;

      for (int i = 0; i < bins.size(); i++) {
        float sum = binSums.get(i) + item.getComputeTime();
        if( sum <= capacity){
          binSums.set(i, sum);
          bins.get(i).add(item.getPartition());
          assigned=true;
          break;
        }
      }

      if(!assigned){
        HashSet<Integer> b=new HashSet<>();
        b.add(item.getPartition());
        bins.add(b);
        binSums.add(item.getComputeTime());
      }
    }

    return bins;

    //TODO: compute the bin to machine mapping
  }

}
