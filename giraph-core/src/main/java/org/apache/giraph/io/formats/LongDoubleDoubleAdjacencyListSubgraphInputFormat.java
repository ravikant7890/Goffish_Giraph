package org.apache.giraph.io.formats;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by anirudh on 30/09/16.
 */
public class LongDoubleDoubleAdjacencyListSubgraphInputFormat extends AdjacencyListTextSubgraphInputFormat<SubgraphId<LongWritable>, SubgraphVertices,
        DoubleWritable> {
    @Override
    public AdjacencyListTextSubgraphReader createVertexReader(InputSplit split,
                                                            TaskAttemptContext context) {
        return new LongDoubleDoubleAdjacencyListSubgraphReader(null);
    }

    /**
     * Vertex reader used with
     * {@link TextDoubleDoubleAdjacencyListVertexInputFormat}
     */
    protected class LongDoubleDoubleAdjacencyListSubgraphReader extends
            AdjacencyListTextSubgraphReader {

        @Override
        public SubgraphEdge<LongWritable, NullWritable, NullWritable> decodeVertexEdge(String id) {
            LongWritable vertexId = new LongWritable(Long.parseLong(id));
            DefaultSubgraphEdge<LongWritable, NullWritable, NullWritable> subgraphEdge = new DefaultSubgraphEdge<>();
            subgraphEdge.initialize(NullWritable.get(), NullWritable.get(), vertexId);
            return subgraphEdge;
        }

        @Override
        public SubgraphVertices getSubgraphVertices() throws IOException, InterruptedException {
            SubgraphVertices<LongWritable, LongWritable, DoubleWritable, DoubleWritable, LongWritable, LongWritable> subgraphVertices = new SubgraphVertices();
            HashMap<LongWritable, SubgraphVertex<LongWritable, LongWritable, DoubleWritable, DoubleWritable, LongWritable>> subgraphVerticesMap = new HashMap<>();
            while (getRecordReader().nextKeyValue()) {
                // take all info from each line

                // Read each vertex
                Text vertexLine = getRecordReader().getCurrentValue();
                String[] processedLine = preprocessLine(vertexLine);

                SubgraphVertex<LongWritable, LongWritable, DoubleWritable, DoubleWritable, LongWritable> subgraphVertex = readVertex(processedLine);
                subgraphVerticesMap.put(subgraphVertex.getId(), subgraphVertex);
            }
            subgraphVertices.initialize(subgraphVerticesMap);
            subgraphVertices.setSubgraphValue(new LongWritable());
            return subgraphVertices;
        }

        @Override
        public Edge<SubgraphId<LongWritable>, DoubleWritable> decodeSubgraphEdge(String value1, String value2) {
            LongWritable sid = new LongWritable(Long.parseLong(value1));
            int pid = Integer.parseInt(value2);
            SubgraphId<LongWritable> subgraphId = new SubgraphId<>(sid, pid);
            Edge <SubgraphId<LongWritable>, DoubleWritable> edge = EdgeFactory.create(subgraphId, new DoubleWritable(0));
            return edge;
        }


        @Override
        public int decodePId(String s) {
            return Integer.parseInt(s);
        }

        /**
         * Constructor with
         * {@link AdjacencyListTextVertexInputFormat.LineSanitizer}.
         *
         * @param lineSanitizer the sanitizer to use for reading
         */
        public LongDoubleDoubleAdjacencyListSubgraphReader(AdjacencyListTextSubgraphInputFormat.LineSanitizer
                                                                 lineSanitizer) {
            super(lineSanitizer);
        }

        @Override
        public LongWritable decodeId(String s) {
            return new LongWritable(Long.parseLong(s));
        }

        @Override
        public LongWritable decodeSId(String s) {
            return new LongWritable(Long.parseLong(s));
        }


        @Override
        public SubgraphVertex readVertex(String[] line) throws IOException{
            SubgraphVertex subgraphVertex = new DefaultSubgraphVertex();
            subgraphVertex.initialize(getVId(line), getValue(line), getVertexEdges(line));
            return subgraphVertex;
        }




        @Override
        public DoubleWritable decodeValue(String s) {
            return new DoubleWritable(Double.parseDouble(s));
        }

        @Override
        public SubgraphId<LongWritable> getSId(String[] line) {
            SubgraphId<LongWritable> subgraphId = new SubgraphId<LongWritable>(decodeSId(line[0]), decodePId(line[1]));
            return subgraphId;
        }
    }
}
