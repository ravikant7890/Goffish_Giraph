package org.apache.giraph.examples;

import org.apache.giraph.graph.*;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by anirudh on 26/01/17.
 */
public class SubgraphSingleSourceShortestPathOutputFormatSir extends
    TextVertexOutputFormat<SubgraphId<LongWritable>,SubgraphVertices,NullWritable> {
  /**
   * Simple text based vertex writer
   */
  private class SimpleTextVertexWriter extends TextVertexWriter {
    @Override
    public void writeVertex(Vertex<SubgraphId<LongWritable>, SubgraphVertices, NullWritable> vertex) throws IOException, InterruptedException {
      Subgraph<LongWritable, LongWritable, LongWritable, NullWritable, ShortestPathSubgraphValue, NullWritable> subgraph = (Subgraph) vertex;
      Map<Long, Short> shortestDistanceMap = subgraph.getSubgraphVertices().getSubgraphValue().shortestDistanceMap;
      for (Map.Entry<Long, Short> entry : shortestDistanceMap.entrySet()) {
        SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable> subgraphVertex = subgraph.getSubgraphVertices().getVertexById(new LongWritable(entry.getKey()));
        if (!subgraphVertex.isRemote()) {
          getRecordWriter().write(
              new Text(entry.getKey().toString()),
              new Text(entry.getValue().toString()));
        }
      }
    }
  }

  @Override
  public TextVertexWriter createVertexWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new SimpleTextVertexWriter();
  }
}