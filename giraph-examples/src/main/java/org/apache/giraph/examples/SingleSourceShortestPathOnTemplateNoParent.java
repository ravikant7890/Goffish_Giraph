/*
 *  Copyright 2013 University of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.package edu.usc.goffish.gopher.sample;
 */
package org.apache.giraph.examples;

import org.apache.giraph.comm.messages.SubgraphMessage;
import org.apache.giraph.graph.*;
import org.apache.giraph.utils.ExtendedByteArrayDataInput;
import org.apache.giraph.utils.ExtendedByteArrayDataOutput;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;


/***
 * Calculates single source shortest path from a single source to every other vertex
 * in the graph. Uses just templates (edge weight = 1).
 * Uses Dikstra/A* algorithm for local calculation within subgraph.
 * When local parents of remote vertices are updated by dikstras', we send remote messages
 * with the updated local parent's distance to remote vertex.
 * We halt when there are no update messages sent to remote vertices.
 * At the end of all supersteps, every vertex has the shortest distance from the source
 * vertex and the parent vertex used to arrive on the shortest path.
 *
 * @author simmhan
 *
 */
public class SingleSourceShortestPathOnTemplateNoParent extends UserSubgraphComputation<LongWritable,
    LongWritable, LongWritable, NullWritable, BytesWritable, ShortestPathSubgraphValue, NullWritable> {

  public static final Logger LOG = Logger.getLogger(SingleSourceShortestPathOnTemplateNoParent.class);
  // Input Variables

  // Output Variables
  // Output shortest distance map

  // dir location where distance results and parents are saved
//  private static Path logRootDir = Paths.get(".");
//  private String logFileName = null;
  //private SimpleDateFormat FORMATTER = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

  // Number of remote vertices out of this subgraph. Used for initializing hashmap.
//  private static int verbosity = -1;
//  long partitionId, subgraphId;

  /***
   * Helper class for items in a sorted priority queue of current vertices that
   * need to be checked for their new distance
   *
   * @author simmhan
   *
   */
  private static class DistanceVertex implements Comparable<DistanceVertex> {
    public short distance;
    public SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable> vertex;

    public DistanceVertex(SubgraphVertex vertex_, short distance_) {
      vertex = vertex_;
      distance = distance_;
    }

    @Override
    public int compareTo(DistanceVertex o) {
      return distance - o.distance;
    }
  }


  /***
   * MAIN COMPUTE METHOD
   */
  @Override
  public void compute(Iterable<SubgraphMessage<LongWritable, BytesWritable>> subgraphMessages) throws IOException {
//    long subgraphStartTime = System.currentTimeMillis();

    Subgraph<LongWritable, LongWritable, LongWritable, NullWritable, ShortestPathSubgraphValue, NullWritable> subgraph = getSubgraph();

    try {
      // init IDs for logging
      // FIXME: Charith, we need an init() method later on
//      if(getSuperstep() == 0) {
//        partitionId = partition.getId();
//        subgraphId = subgraph.getId();
//        logFileName = "SP_" + partitionId + "_" + subgraphId + ".log";
//      }

//      log("START superstep with received input messages count = " + packedSubGraphMessages.size());

      Set<SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable>> rootVertices = null;

      ///////////////////////////////////////////////////////////
      // First superstep. Get source superstep as input.
      // Initialize distances. calculate shortest distances in subgraph.
      if (getSuperstep() == 0) {

        // get input variables from init message
//        if(packedSubGraphMessages.size() == 0) {
//          throw new RuntimeException("Initial subgraph message was missing! Require sourceVertexID to be passed");
//        }

        long sourceVertexID = getConf().getSubgraphSourceVertex();

//        log("Initializing source vertex = " + sourceVertexID);

        // Giraph:SimpleShortestPathsComputation.java:64
        //   vertex.setValue(new DoubleWritable(Double.MAX_VALUE));

        // initialize distance map of vertices to infinity
        // Note that if it is a remote vertex, we only have an estimate of the distance
        ShortestPathSubgraphValue subgraphValue = new ShortestPathSubgraphValue();
        subgraph.getSubgraphVertices().setSubgraphValue(subgraphValue);
        subgraphValue.shortestDistanceMap = new HashMap<Long, Short>((int) subgraph.getSubgraphVertices().getNumVertices());
        for (SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable> v : subgraph.getSubgraphVertices().getLocalVertices().values()) {
          subgraphValue.shortestDistanceMap.put(v.getId().get(), Short.MAX_VALUE);
        }

        for (SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable> v : subgraph.getSubgraphVertices().getRemoteVertices().values()) {
          subgraphValue.shortestDistanceMap.put(v.getId().get(), Short.MAX_VALUE);
//          remoteVertexCount++;
        }

        // Giraph:SimpleShortestPathsComputation.java:66
        //    double minDist = isSource(vertex) ? 0d : Double.MAX_VALUE;
        // Update distance to source as 0
        boolean subgraphHasSource = false;
        if (subgraphValue.shortestDistanceMap.containsKey(sourceVertexID) &&
            !subgraph.getSubgraphVertices().getVertexById(new LongWritable(sourceVertexID)).isRemote()) {
          subgraphValue.shortestDistanceMap.put(sourceVertexID, (short) 0);
          subgraphHasSource = true;
        }

        // If we have the source...
        if (subgraphHasSource) {
          SubgraphVertex sourceVertex = subgraph.getSubgraphVertices().getVertexById(new LongWritable(sourceVertexID));
          rootVertices = new HashSet<>(1);
          rootVertices.add(sourceVertex);
        }

      } else {
        ///////////////////////////////////////////////////////////
        // second superstep.
        int size = 0;
        for (SubgraphMessage<LongWritable, BytesWritable> subgraphMessage : subgraphMessages) {
          size += subgraphMessage.getMessage().getLength();
        }
        // min(getLocalVertices, messsage length / ((8+2)/2))
        rootVertices = new HashSet<>(Math.min((int) subgraph.getSubgraphVertices().getNumVertices(), size / 5));
        unpackSubgraphMessages(subgraphMessages, subgraph, rootVertices);
//        log("Unpacked messages count = " + subGraphMessages.size());

//
//        // We expect no more unique vertices than the number of input messages,
//        // or the total number of vertices. Note that we are likely over allocating.
//        // For directed graphs, it is not easy to find the number of in-boundary vertices.
//        rootVertices = new HashSet<>(Math.min(subGraphMessages.size(), (int) subgraph.getSubgraphVertices().getNumVertices()));
//
//        // Giraph:SimpleShortestPathsComputation.java:68
//        //     minDist = Math.min(minDist, message.get());
//
//        // parse messages
//        // update distance map using messages if it has improved
//        // add the *unique set* of improved vertices to traversal list
//        for (String message : subGraphMessages) {
//          String[] tokens  = message.split(",");
//          if(tokens.length != 2) {
//            throw new RuntimeException("Intermediate subgraph message did not contain 2 tokens. Has " + tokens.length + "instead");
//          }
//          long sinkVertex = Long.parseLong(tokens[0]);
//          short sinkDistance = Short.parseShort(tokens[1]);
//          short distance = shortestDistanceMap.get(sinkVertex);
//          if(distance > sinkDistance){
//            // path from remote is better than locally known path
//            shortestDistanceMap.put(sinkVertex, sinkDistance);
//            rootVertices.add(subgraph.getSubgraphVertices().getVertexById(new LongWritable(sinkVertex));
//          }
//        }
      }

      ShortestPathSubgraphValue subgraphValue = subgraph.getSubgraphVertices().getSubgraphValue();
      // Giraph:SimpleShortestPathsComputation.java:74
      //     if (minDist < vertex.getValue().get()) {
      //       vertex.setValue(new DoubleWritable(minDist));
      //       for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
      //         double distance = minDist + edge.getValue().get();
      //

      // if there are changes, then run dikstras

      if (rootVertices != null && rootVertices.size() > 0) {
        // List of remote vertices which could be affected by changes to distance
        // This does local agg that eliminates sending min dist to same vertex from
        // multiple vertices in this SG
        Set<Long> remoteUpdateSet = new HashSet<Long>(subgraph.getSubgraphVertices().getRemoteVertices().size());

        // Update distances within local subgraph
        // Get list of remote vertices that were reached and updated.
        aStar(rootVertices, subgraphValue.shortestDistanceMap, remoteUpdateSet, subgraph);

//        log("END diskstras with subgraph local vertices="+
//            (subgraph.getSubgraphVertices().getNumVertices() - remoteVertexCount) + "," +logMsg);

        // Giraph:SimpleShortestPathsComputation.java:82
        //     sendMessage(edge.getTargetVertexId(), new DoubleWritable(distance));

        // Notify remote vertices of new known shortest distance from this subgraph and parent.
//    		for(Long remoteVertexID : remoteUpdateSet){
//    			String payload = remoteVertexID + "," + shortestDistanceMap.get(remoteVertexID).toString();
//    			SubGraphMessage msg = new SubGraphMessage(payload.getBytes());
//    			msg.setTargetSubgraph(subgraph.getVertex(remoteVertexID).getRemoteSubgraphId());
//    			sendMessage(msg);
//                changeCount++;
//    		}

        // Aggregate messages to remote subgraph
        packAndSendMessages(remoteUpdateSet, subgraph);
      }

//      log("END superstep. Sent remote vertices = " + changeCount + ", remote messages =" + messageCount);

      // if no distances were changed, we terminate.
      // if no one's distances change, everyone has votd to halt
//        if(changeCount == 0) {
      // we're done
      voteToHalt();
//        }


    } catch (RuntimeException ex) {
//      if(logFileName == null) logFileName = "ERROR.log";
//      log("Unknown error in compute", ex);
    }

    long subgraphEndTime = System.currentTimeMillis();

//    logPerfString("SUBGRAPH_PERF ,"+subgraph.getId() +" ," + getSuperStep() + " ," +getIteration() + " ,"+  subgraphStartTime
//        + " ,"+subgraphEndTime + " ," +  (subgraphEndTime - subgraphStartTime)+ " ,"+subgraph.numVertices() + " ," + subgraph.numEdges());

  }

//  public void wrapup(){
//
//    ///////////////////////////////////////////////
//    /// Log the distance map
//    // FIXME: Charith, we need an finally() method later on
//    try {
//      Path filepath = logRootDir.resolve("from-" + sourceVertexID + "-pt-" + partition.getId() + "-sg-"+subgraph.getId()+"-" + superStep + ".sssp");
//      System.out.println("Writing mappings to file " + filepath);
//      File file = new File(filepath.toString());
//      PrintWriter writer = new PrintWriter(file);
//      writer.println("# Source vertex,"+sourceVertexID);
//      writer.println("## Sink vertex, Distance");
//      for(ITemplateVertex v : subgraph.vertices()){
//        if(!v.isRemote()) { // print only non-remote vertices
//          short distance = shortestDistanceMap.get(v.getId());
//          if(distance != Short.MAX_VALUE) // print only connected vertices
//            writer.println(v.getId() + "," + distance);
//        }
//      }
//      writer.flush();
//      writer.close();
//    } catch (FileNotFoundException e) {
//      e.printStackTrace();
//    }
//  }

  void packAndSendMessages(Set<Long> remoteVertexUpdates, Subgraph<LongWritable, LongWritable, LongWritable, NullWritable, ShortestPathSubgraphValue, NullWritable> subgraph) throws IOException {
    ShortestPathSubgraphValue subgraphValue = subgraph.getSubgraphVertices().getSubgraphValue();
    HashMap<SubgraphId<LongWritable>, ExtendedByteArrayDataOutput> messagesMap = new HashMap<>();
    int unpackedMessageCount = 0;
    for (long entry : remoteVertexUpdates) {
      unpackedMessageCount++;
      RemoteSubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable> remoteSubgraphVertex = (RemoteSubgraphVertex) subgraph.getSubgraphVertices().getVertexById(new LongWritable(entry));
      ExtendedByteArrayDataOutput dataOutput;
      if (!messagesMap.containsKey(remoteSubgraphVertex.getSubgraphId())) {
        dataOutput = new ExtendedByteArrayDataOutput();
        messagesMap.put(remoteSubgraphVertex.getSubgraphId(), dataOutput);
      } else {
        dataOutput = messagesMap.get(remoteSubgraphVertex.getSubgraphId());
      }
      dataOutput.writeLong(remoteSubgraphVertex.getId().get());
      dataOutput.writeShort(subgraphValue.shortestDistanceMap.get(entry));
      // LOG.info("SubgraphID" + remoteSubgraphVertex.getSubgraphId() + " Sending vertex id " + remoteSubgraphVertex.getId().get() + " distance "+ entry.getValue());
    }
    for (Map.Entry<SubgraphId<LongWritable>, ExtendedByteArrayDataOutput> entry : messagesMap.entrySet()) {
      ExtendedByteArrayDataOutput dataOutput = entry.getValue();
      dataOutput.writeLong(-1);
      sendMessage(entry.getKey(), new BytesWritable(dataOutput.getByteArray()));
    }
    LOG.info("Superstep,SubgraphId,unpackedSentMessageCount,packedSentMessageCount:" + getSuperstep() + "," + subgraph.getSubgraphId() + "," + unpackedMessageCount + "," + messagesMap.size());
  }


  private void unpackSubgraphMessages(
      Iterable<SubgraphMessage<LongWritable, BytesWritable>> packedSubGraphMessages, Subgraph<LongWritable, LongWritable, LongWritable, NullWritable, ShortestPathSubgraphValue, NullWritable> subgraph, Set<SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable>> rootVertices) throws IOException {
    ShortestPathSubgraphValue subgraphValue = subgraph.getSubgraphVertices().getSubgraphValue();
    int unpackedMessageCount = 0;
    int packedMessageCount = 0;
    for (SubgraphMessage<LongWritable, BytesWritable> subgraphMessage : packedSubGraphMessages) {
      packedMessageCount++;
      BytesWritable subgraphMessageValue = subgraphMessage.getMessage();
      ExtendedByteArrayDataInput dataInput = new ExtendedByteArrayDataInput(subgraphMessageValue.getBytes());
      while (!dataInput.endOfInput()) {
        long sinkVertex = dataInput.readLong();
        if (sinkVertex == -1) {
          break;
        }
        unpackedMessageCount++;
        short sinkDistance = dataInput.readShort();
        //LOG.info("Test, Sink vertex received: " + sinkVertex);
//        SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable> currentVertex = vertices.get(new LongWritable(sinkVertex));
        //LOG.info("Test, Current vertex object: " + currentVertex);

        //LOG.info("Test, Current vertex: " + currentVertex.getId());
        short distance = subgraphValue.shortestDistanceMap.get(sinkVertex);
        if (sinkDistance < distance) {
          subgraphValue.shortestDistanceMap.put(sinkVertex, sinkDistance);
          rootVertices.add(subgraph.getSubgraphVertices().getVertexById(new LongWritable(sinkVertex)));
        }
      }
    }
    LOG.info("Superstep,SubgraphId,unpackedReceivedMessageCount,packedReceivedMessageCount,rootVertices:" + getSuperstep() + "," + subgraph.getSubgraphId() + "," + unpackedMessageCount + "," + packedMessageCount + "," + rootVertices.size());
  }


  /***
   * Calculate (updated) distances and their parents based on traversals starting at "root"
   * If remote vertices were reached, add them to remote update set and return.
   * This is similar to the A* algorithm pattern. This method is not thread safe since
   * the shortestDistanceMap and the remoteUpdateSet are modified.
   * The algorithm is run on the template by traversing from the rootVertices,
   * and the edge weights are assumed to be 1.
   * @param rootVertices the initial set of vertices that have external updates
   * @param shortestDistanceMap a map from the list of vertices to their shortest known distance+parent.
   * 			This is passed as input and also updated by this method.
   * @param remoteUpdateSet a list of remote vertices whose parent distances have changed.
   * @param subgraph
   */
  public void aStar(
      Set<SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable>> rootVertices,
      Map<Long, Short> shortestDistanceMap,
      Set<Long> remoteUpdateSet, Subgraph<LongWritable, LongWritable, LongWritable, NullWritable, ShortestPathSubgraphValue, NullWritable> subgraph) {

    // add root vertex whose distance was updated to the sorted distance list
    // assert rootVertex.isRemote() == false

    // queue of vertices to traverse, sorted by shortest known distance
    // We are simulating a ordered set using a hashmap (to test uniqueness) and priority queue (for ordering)
    // Note that SortedSet does not allow comparable and equals to be inconsistent.
    // i.e. we need equals to operate on vertex ID while comparator to operate on vertex distance
    // NOTE: Maybe using TreeSet with Comparator passed in constructor may work better?
    PriorityQueue<DistanceVertex> localUpdateQueue = new PriorityQueue<>();
    Map<Long, DistanceVertex> localUpdateMap = new HashMap<>();
    for (SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable> rootVertex : rootVertices) {
      short rootDistance = shortestDistanceMap.get(rootVertex.getId().get());
      DistanceVertex distanceVertex = new DistanceVertex(rootVertex, rootDistance);
      localUpdateQueue.add(distanceVertex);
      localUpdateMap.put(rootVertex.getId().get(), distanceVertex);
    }


    SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable> currentVertex;
    DistanceVertex currentDistanceVertex;

    // FIXME:TEMPDEL: temporary variable for logging
    long localUpdateCount = 0, incrementalChangeCount = 0;

    // pick the next vertex with shortest distance
    long count = 0;
    while ((currentDistanceVertex = localUpdateQueue.poll()) != null) { // remove vertex from queue
      localUpdateMap.remove(currentDistanceVertex.vertex.getId().get()); // remote vertex from Map
      localUpdateCount++; // FIXME:TEMPDEL

      // get the shortest distance for the current vertex
      currentVertex = currentDistanceVertex.vertex;
      // calculate potential new distance for all children
      short newChildDistance = (short) (currentDistanceVertex.distance + 1); // FIXME: this will not work for weighted edges

      // BFS traverse to children of current vertex
      // update their shortest distance if necessary
      // add them to update set if distance has changed
      for (SubgraphEdge<LongWritable, NullWritable, NullWritable> e : currentVertex.getOutEdges()) {

        // get child vertex
        SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable> childVertex = subgraph.getSubgraphVertices().getVertexById(e.getSinkVertexId());
        long childVertexID = childVertex.getId().get();
        short childDistance = shortestDistanceMap.get(childVertexID);

        // update distance to childVertex if it has improved
        if (childDistance > newChildDistance) {
          if (childDistance != Short.MAX_VALUE) incrementalChangeCount++;

          shortestDistanceMap.put(childVertexID, newChildDistance);

          // if child is a remote vertex, then update its "local" shortest path.
          // note that we don't know what its global shortest path is.
          if (childVertex.isRemote()) {
            // add to remote update set ...
            remoteUpdateSet.add(childVertexID);
          } else {
            // if child does not exist, add to queue and map
            if (!localUpdateMap.containsKey(childVertexID)) {
              DistanceVertex childDistanceVertex = new DistanceVertex(childVertex, newChildDistance);
              localUpdateQueue.add(childDistanceVertex);
              localUpdateMap.put(childVertexID, childDistanceVertex);

            } else {
              // else update priority queue
              DistanceVertex childDistanceVertex = localUpdateMap.get(childVertexID);
              localUpdateQueue.remove(childDistanceVertex);
              childDistanceVertex.distance = newChildDistance;
              localUpdateQueue.add(childDistanceVertex);
            }
          }
        } // end if better path
      } // end edge traversal
      count++;

      // verbose
//      if(verbosity > 0) {
//        if((count % 100) == 0) System.out.print(".");
//        if((count % 1000) == 0) System.out.println("@"+localUpdateQueue.size());
//      }

    } // end vertex traversal

    // FIXME:TEMPDEL
    LOG.info("Superstep,SubgraphId,localUpdate,incrementalChangeCount:" + getSuperstep() + "," + subgraph.getSubgraphId() + "," + localUpdateCount + "," + incrementalChangeCount);
  }



  private void log(String message, Exception ex) {
//        try(PrintWriter writer = new PrintWriter(new FileOutputStream(logRootDir.resolve(logFileName).toFile(), true))){
//
//	        writer.println(System.currentTimeMillis()+":"+partitionId + ":" + subgraphId + ":" + superStep + ": ERROR! " + message);
//	        ex.printStackTrace(writer);
//	        writer.flush();
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
  }

  void logPerfString(String str) {
    System.out.println(str);
  }

}