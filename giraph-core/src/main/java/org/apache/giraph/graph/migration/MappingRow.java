package org.apache.giraph.graph.migration;

/**
 * Created by anirudh on 03/02/17.
 */
public class MappingRow {
  private int superstep;
  private int partition;
  private float time;

  public MappingRow(int superstep, int partition, float time) {
    this.superstep = superstep;
    this.partition = partition;
    this.time = time;
  }

  public float getComputeTime() {
    return time;
  }

  public int getPartition() {
    return partition;
  }


}
