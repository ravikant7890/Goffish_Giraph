package org.apache.giraph.graph;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;

/**
 * Created by anirudh on 02/11/16.
 */
public interface SubgraphVertex<S extends WritableComparable, I extends WritableComparable, V extends Writable, E extends Writable, EI extends WritableComparable> extends WritableComparable {
  LinkedList<SubgraphEdge<I, E, EI>> getOutEdges();

  @Override
  int compareTo(Object o);

  I getId();

  void setId(I id);

  V getValue();

  void setValue(V value);

  boolean isRemote();

  void initialize(I vertexId, V value, LinkedList<SubgraphEdge<I, E, EI>> edges);

  @Override
  void write(DataOutput dataOutput) throws IOException;

  void readFields(ImmutableClassesGiraphConfiguration conf, DataInput dataInput) throws IOException;
}
