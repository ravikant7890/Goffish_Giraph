package org.apache.giraph.graph;

import org.apache.giraph.comm.messages.SubgraphMessage;
import org.apache.giraph.conf.GiraphConfigurationSettable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;

/**
 * Created by anirudh on 26/02/17.
 */
public abstract class UserSubgraphComputation<S extends WritableComparable,
    I extends WritableComparable, V extends WritableComparable, E extends Writable, M extends Writable, SV extends Writable, EI extends WritableComparable>
    implements GiraphConfigurationSettable {

  private ImmutableClassesGiraphConfiguration conf;

  private GiraphSubgraphComputation<S, I, V, E, M, SV, EI> giraphSubgraphComputation;

  public long getSuperstep() {
    return giraphSubgraphComputation.getSuperstep() - 3;
  }

  void setGiraphSubgraphComputation(GiraphSubgraphComputation<S, I, V, E, M, SV, EI> giraphSubgraphComputation) {
    this.giraphSubgraphComputation = giraphSubgraphComputation;
  }

  public Subgraph<S, I, V, E, SV, EI> getSubgraph() {
    return giraphSubgraphComputation.getSubgraph();
  }

  public void voteToHalt() {
    giraphSubgraphComputation.voteToHalt();
  }

  public abstract void compute(Iterable<SubgraphMessage<S, M>> messages) throws IOException;

  public void sendMessage(SubgraphId<S> subgraphId, M message) {
    giraphSubgraphComputation.sendMessage(subgraphId, message);
  }

//  void sendToVertex(I vertexID, M message);

//  void sendToAll(M message); // auto fill subgraph ID on send or receive


  public void sendToNeighbors(M message) {
    giraphSubgraphComputation.sendMessageToAllNeighboringSubgraphs(message);
  }

  @Override
  public void setConf(ImmutableClassesGiraphConfiguration configuration) {
    conf = configuration;
  }

  public ImmutableClassesGiraphConfiguration getConf() {
    return conf;
  }

  void sendMessage(SubgraphId<S> subgraphID, Iterable<M> message) {
    throw new UnsupportedOperationException();
  }


  void sendToAll(Iterable<M> message) {
    throw new UnsupportedOperationException();
  }

  void sendToNeighbors(Iterable<M> message) {
    throw new UnsupportedOperationException();
  }

}
