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
public class SubgraphSingleSourceShortestPathOutputFormat extends
    TextVertexOutputFormat<SubgraphId<LongWritable>,SubgraphVertices,NullWritable> {
  /**
   * Simple text based vertex writer
   */
  private class SimpleTextVertexWriter extends TextVertexWriter {
    @Override
    public void writeVertex(Vertex<SubgraphId<LongWritable>, SubgraphVertices, NullWritable> vertex) throws IOException, InterruptedException {
      Subgraph<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable, NullWritable> subgraph = (Subgraph) vertex;
      HashMap<LongWritable, SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable>> vertices = subgraph.getSubgraphVertices().getLocalVertices();
      for (Map.Entry<LongWritable, SubgraphVertex<LongWritable, LongWritable, LongWritable, NullWritable, NullWritable>> entry : vertices.entrySet()) {
        getRecordWriter().write(
            new Text(String.valueOf(entry.getKey().get())),
            new Text(entry.getValue().getValue().toString()));
      }
    }
  }

  @Override
  public TextVertexWriter createVertexWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new SimpleTextVertexWriter();
  }
}
