package org.apache.giraph.examples;

/**
 * Created by anirudh on 06/02/17.
 */

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.giraph.comm.messages.SubgraphMessage;
import org.apache.giraph.graph.*;
import org.apache.giraph.utils.ExtendedByteArrayDataInput;
import org.apache.giraph.utils.ExtendedByteArrayDataOutput;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

/*
 * Ported from goffish v2
 */

/**
 * Counts and lists all the triangles found in an undirected graph. A triangle
 * can be classified into three types based on the location of its vertices: i)
 * All the vertices lie in the same partition, ii) two of the vertices lie in
 * the same partition and iii) all the vertices lie in different partitions. (i)
 * and (ii) types of triangle can be identified with the information available
 * within the subgraph. For (iii) type of triangles three supersteps are
 * required.
 *
 * @author Diptanshu Kakwani
 * @version 1.0
 * @see <a href="http://www.dream-lab.in/">DREAM:Lab</a>
 * <p>
 * Copyright 2014 DREAM:Lab, Indian Institute of Science, Bangalore
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class SubgraphTriangleCount extends
    UserSubgraphComputation<LongWritable, LongWritable, LongWritable, NullWritable, BytesWritable, TriangleCountSubgraphValue, LongWritable> {

  @Override
  public void compute(Iterable<SubgraphMessage<LongWritable, BytesWritable>> subgraphMessages) throws IOException {
    // Convert adjacency list to adjacency set
    Subgraph<LongWritable, LongWritable, LongWritable, NullWritable, TriangleCountSubgraphValue, LongWritable> subgraph = getSubgraph();
    if (getSuperstep() == 0) {
      TriangleCountSubgraphValue triangleCountSubgraphValue = new TriangleCountSubgraphValue();
      triangleCountSubgraphValue.adjSet = new HashMap<Long, Set<Long>>();
      subgraph.getSubgraphVertices().setSubgraphValue(triangleCountSubgraphValue);
      for (SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, LongWritable> vertex : subgraph.getSubgraphVertices().getLocalVertices().values()) {
        Set<Long> adjVertices = new HashSet<Long>();
        for (SubgraphEdge<LongWritable, NullWritable, LongWritable> edge : vertex.getOutEdges()) {
          adjVertices.add(edge.getSinkVertexId().get());
        }
        triangleCountSubgraphValue.adjSet.put(vertex.getId().get(), adjVertices);
      }
      return;
    }
    else if(getSuperstep() == 1) {
      TriangleCountSubgraphValue triangleCountSubgraphValue = subgraph.getSubgraphVertices().getSubgraphValue();
      long triangleCount = triangleCountSubgraphValue.triangleCount;
      Map<SubgraphId<LongWritable>, ExtendedByteArrayDataOutput> msg = new HashMap<>();
      for (SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, LongWritable> vertex : subgraph.getSubgraphVertices().getLocalVertices().values()) {
        for (SubgraphEdge<LongWritable, NullWritable, LongWritable> edge : vertex.getOutEdges()) {
          SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, LongWritable> adjVertex =
              subgraph.getSubgraphVertices().getVertexById(edge.getSinkVertexId());

          // Preparing messages to be sent to remote adjacent vertices.
          if (adjVertex.isRemote() && adjVertex.getId().get() > vertex.getId().get()) {
            SubgraphId<LongWritable> remoteSubgraphId = ((RemoteSubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, LongWritable>) adjVertex)
                .getSubgraphId();
            ExtendedByteArrayDataOutput vertexIds = msg.get(remoteSubgraphId);
            if (vertexIds == null) {
              vertexIds = new ExtendedByteArrayDataOutput();
              msg.put(remoteSubgraphId, vertexIds);
            }
            vertexIds.writeLong(adjVertex.getId().get());
            vertexIds.writeLong(vertex.getId().get());
            vertexIds.writeLong(vertex.getId().get());

          } else if (adjVertex.isRemote() || vertex.getId().get() > adjVertex.getId().get())
            continue;

          if (adjVertex.isRemote()) {
            continue;  //as it has no outedges
          }
          // Counting triangles which have at least two vertices in the same
          // subgraph.
          for (SubgraphEdge<LongWritable, NullWritable, LongWritable> edgeAdjVertex : adjVertex.getOutEdges()) {
            SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, LongWritable> adjAdjVertex = subgraph.getSubgraphVertices().getVertexById(edgeAdjVertex.getSinkVertexId());
            if (adjAdjVertex.isRemote()
                || adjAdjVertex.getId().get() > adjVertex.getId().get()) {
              if (triangleCountSubgraphValue.adjSet.get(vertex.getId().get()).contains(adjAdjVertex.getId().get())) {
                triangleCount++;
                //trianglesList.append(vertex.getVertexID().get() + " " + adjVertex.getVertexID().get()
                //  + " " + adjAdjVertex.getVertexID().get() + "\n");
              }
            }
          }
        }
      }
      triangleCountSubgraphValue.triangleCount = triangleCount;
      sendPackedMessages(msg);
    } else if (getSuperstep() == 2) {
      Map<Long, List<Pair<Long, Long>>> ids = new HashMap<Long, List<Pair<Long, Long>>>();
      unpackMessages(subgraphMessages, ids);

      Map<SubgraphId<LongWritable>, ExtendedByteArrayDataOutput> msg = new HashMap<>();
      for (Map.Entry<Long, List<Pair<Long, Long>>> entry : ids.entrySet()) {
        SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, LongWritable> vertex = subgraph.getSubgraphVertices().getVertexById(new LongWritable(entry.getKey()));
        List<Pair<Long, Long>> idPairs = entry.getValue();
        for (SubgraphEdge<LongWritable, NullWritable, LongWritable> edge : vertex.getOutEdges()) {
          SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, LongWritable> adjVertex = subgraph.getSubgraphVertices().getVertexById(edge.getSinkVertexId());
          if (adjVertex.isRemote() && adjVertex.getId().get() > vertex.getId().get()) {
            SubgraphId<LongWritable> remoteSubgraphId = ((RemoteSubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, LongWritable>) adjVertex)
                .getSubgraphId();
            ExtendedByteArrayDataOutput vertexIds = msg.get(remoteSubgraphId);
            if (vertexIds == null) {
              vertexIds = new ExtendedByteArrayDataOutput();
              msg.put(remoteSubgraphId, vertexIds);
            }
            for (Pair<Long, Long> id : idPairs) {
              LongWritable firstId = new LongWritable(id.first);
              RemoteSubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, LongWritable> sinkSubgraphID = (RemoteSubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, LongWritable>)
                  subgraph.getSubgraphVertices().getVertexById(firstId);
              if (sinkSubgraphID.getSubgraphId() != remoteSubgraphId) {
                vertexIds.writeLong(adjVertex.getId().get());
                vertexIds.writeLong(firstId.get());
                vertexIds.writeLong(vertex.getId().get());
              }
            }
          }
        }
      }
      sendPackedMessages(msg);

    } else {
      long triangleCount = subgraph.getSubgraphVertices().getSubgraphValue().triangleCount;
      Map<Long, List<Pair<Long, Long>>> ids = new HashMap<Long, List<Pair<Long, Long>>>();
      unpackMessages(subgraphMessages, ids);
      for (Map.Entry<Long, List<Pair<Long, Long>>> entry : ids.entrySet()) {
        SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, LongWritable> vertex = subgraph.getSubgraphVertices().getLocalVertices().get(new LongWritable(entry.getKey()));
        for (Pair<Long, Long> p : entry.getValue()) {
          for (SubgraphEdge<LongWritable, NullWritable, LongWritable> edge : vertex.getOutEdges()) {
            if (edge.getSinkVertexId().get() == p.first) {
              triangleCount++;
            }
          }
        }

      }
      subgraph.getSubgraphVertices().getSubgraphValue().triangleCount = triangleCount;
    }
    voteToHalt();
  }

  // To represent sender and message content.
  private class Pair<L, R> {
    L first;
    R second;

    Pair(L a, R b) {
      first = a;
      second = b;
    }
  }

  void sendPackedMessages(Map<SubgraphId<LongWritable>, ExtendedByteArrayDataOutput> msg) throws IOException {
    for (Map.Entry<SubgraphId<LongWritable>, ExtendedByteArrayDataOutput> m : msg.entrySet()) {
      m.getValue().writeLong(-1);
      sendMessage(m.getKey(), new BytesWritable(m.getValue().getByteArray()));
    }
  }

  /*
   * Unpacks the messages such that there is a list of pair of message vertex id
   * and source vertex Ids associated with the each target vertex.
   */
  void unpackMessages(Iterable<SubgraphMessage<LongWritable, BytesWritable>> subgraphMessages,
                      Map<Long, List<Pair<Long, Long>>> ids) throws IOException {
    for (SubgraphMessage<LongWritable, BytesWritable> messageItem : subgraphMessages) {
      BytesWritable message = messageItem.getMessage();
      ExtendedByteArrayDataInput dataInput = new ExtendedByteArrayDataInput(message.getBytes());
      Long targetId;
      while((targetId=dataInput.readLong()) != -1) {
        Long messageId = dataInput.readLong();
        Long sourceId = dataInput.readLong();
        List<Pair<Long, Long>> idPairs = ids.get(targetId);
        if (idPairs == null) {
          idPairs = new LinkedList<Pair<Long, Long>>();
          ids.put(targetId, idPairs);
        }
        idPairs.add(new Pair<Long, Long>(messageId, sourceId));
      }
    }
  }

}