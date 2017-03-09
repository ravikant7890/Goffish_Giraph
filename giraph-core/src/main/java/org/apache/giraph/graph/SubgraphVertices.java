package org.apache.giraph.graph;

import com.google.common.collect.Lists;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Created by anirudh on 27/09/16.
 *
 * @param <S> Subgraph id
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */

public class SubgraphVertices<S extends WritableComparable,
    I extends WritableComparable, V extends Writable, E extends Writable, SV extends Writable, EI extends WritableComparable> implements Writable {
  private long numVertices;

  private HashMap<I, RemoteSubgraphVertex<S, I, V, E, EI>> remoteVertices;
  private SV subgraphValue;
  private HashMap<I, SubgraphVertex<S, I, V, E, EI>> vertices;

  public SubgraphVertices() {
////        System.out.println("Calling subgraph vertices constructor");
//        try {
////            System.out.println("Inside try");
//            throw new Exception("Calling constructor");
//        } catch(Exception e) {
////            System.out.println("Inside catch");
////            e.printStackTrace(System.out);
//            e.printStackTrace();
//        }
  }

  public HashMap<I, RemoteSubgraphVertex<S, I, V, E, EI>> getRemoteVertices() {
    return remoteVertices;
  }

  public long getNumRemoteVertices() {
    return (long) remoteVertices.size();
  }

  public void setRemoteVertices(HashMap<I, RemoteSubgraphVertex<S, I, V, E, EI>> remoteVertices) {
    this.remoteVertices = remoteVertices;
  }

  public Iterable<SubgraphVertex<S, I, V, E, EI>> getVertices() {
    return new Iterable<SubgraphVertex<S, I, V, E, EI>>() {

      private Iterator<SubgraphVertex<S, I, V, E, EI>> localVertexIterator = vertices.values().iterator();
      private Iterator<RemoteSubgraphVertex<S, I, V, E, EI>> remoteVertexIterator = remoteVertices.values().iterator();

      @Override
      public Iterator<SubgraphVertex<S, I, V, E, EI>> iterator() {
        return new Iterator<SubgraphVertex<S, I, V, E, EI>>() {
          @Override
          public boolean hasNext() {
            if (localVertexIterator.hasNext()) {
              return true;
            } else {
              return remoteVertexIterator.hasNext();
            }
          }

          @Override
          public SubgraphVertex<S, I, V, E, EI> next() {
            if (localVertexIterator.hasNext()) {
              return localVertexIterator.next();
            } else {
              return remoteVertexIterator.next();
            }
          }

          @Override
          public void remove() {

          }
        };
      }
    };
  }

  public SV getSubgraphValue() {
    return subgraphValue;
  }

  public void setSubgraphValue(SV subgraphValue) {
    this.subgraphValue = subgraphValue;
  }

  public long getNumVertices() {
    return vertices.size();
  }

  public HashMap<I, SubgraphVertex<S, I, V, E, EI>> getLocalVertices() {
    return vertices;
  }

  public SubgraphVertex<S, I, V, E, EI> getVertexById(I vertexId) {
    SubgraphVertex<S, I, V, E, EI> subgraphVertex = vertices.get(vertexId);
    return subgraphVertex != null ? subgraphVertex : remoteVertices.get(vertexId);
  }

  public void initialize(HashMap<I, SubgraphVertex<S, I, V, E, EI>> vertices) {
    this.vertices = vertices;
    this.remoteVertices = new HashMap<>();
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    subgraphValue.write(dataOutput);
    dataOutput.writeInt(vertices.size());
    for (SubgraphVertex<S, I, V, E, EI> vertex : vertices.values()) {
      vertex.write(dataOutput);
    }
    dataOutput.writeInt(remoteVertices.size());
    for (RemoteSubgraphVertex<S, I, V, E, EI> vertex : remoteVertices.values()) {
      vertex.write(dataOutput);
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    throw new UnsupportedOperationException("read fields without conf is not supported");
  }


  public void readFields(ImmutableClassesGiraphConfiguration conf, DataInput dataInput) throws IOException {
//        try {
////            System.out.println("Read field for subgraph vertices with conf");
//            throw new Exception();
//        } catch(Exception e) {
////            System.out.println("Calling readFields with conf");
//            e.printStackTrace(System.out);
//            e.printStackTrace();
//        }
    subgraphValue = (SV) conf.createSubgraphValue();
    subgraphValue.readFields(dataInput);
    int numVertices = dataInput.readInt();
    vertices = new HashMap<>();
    for (int i = 0; i < numVertices; i++) {
      SubgraphVertex<S, I, V, E, EI> subgraphVertex = new DefaultSubgraphVertex<S, I, V, E, EI>();
      subgraphVertex.readFields(conf, dataInput);
      vertices.put(subgraphVertex.getId(), subgraphVertex);
    }
    remoteVertices = new HashMap<>();
    int numRemoteVertices = dataInput.readInt();
    for (int i = 0; i < numRemoteVertices; i++) {
      RemoteSubgraphVertex<S, I, V, E, EI> remoteSubgraphVertex = new DefaultRemoteSubgraphVertex<>();
      remoteSubgraphVertex.readFields(conf, dataInput);
      remoteVertices.put(remoteSubgraphVertex.getId(), remoteSubgraphVertex);
    }
  }

}
