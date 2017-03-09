package org.apache.giraph.examples;

/**
 * Created by anirudh on 09/02/17.
 */
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Simple text-based {@link org.apache.giraph.io.EdgeInputFormat} for
 * unweighted graphs with long ids.
 *
 * Each line consists of: <src id> <target id>
 */
public class LongNullTextEdgeInputFormat extends
    TextEdgeInputFormat<LongWritable, NullWritable> {
  /** Splitter for endpoints */
  private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

  @Override
  public EdgeReader<LongWritable, NullWritable> createEdgeReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    return new LongNullTextEdgeReader();
  }

  /**
   * {@link org.apache.giraph.io.EdgeReader} associated with
   * {@link LongNullTextEdgeInputFormat}.
   */
  public class LongNullTextEdgeReader extends
      TextEdgeReaderFromEachLineProcessed<String[]> {
    @Override
    protected String[] preprocessLine(Text line) throws IOException {
      return SEPARATOR.split(line.toString());
    }

    @Override
    protected LongWritable getSourceVertexId(String[] tokens)
        throws IOException {
      return new LongWritable(Long.parseLong(tokens[0]));
    }

    @Override
    protected LongWritable getTargetVertexId(String[] tokens)
        throws IOException {
      return new LongWritable(Long.parseLong(tokens[1]));
    }

    @Override
    protected NullWritable getValue(String[] tokens) throws IOException {
      return NullWritable.get();
    }
  }

  /**
   * A pair of longs
   */
  public class LongPair {
    /** First element. */
    private long first;
    /** Second element. */
    private long second;

    /** Constructor.
     *
     * @param fst First element
     * @param snd Second element
     */
    public LongPair(long fst, long snd) {
      first = fst;
      second = snd;
    }

    /**
     * Get the first element.
     *
     * @return The first element
     */
    public long getFirst() {
      return first;
    }

    /**
     * Set the first element.
     *
     * @param first The first element
     */
    public void setFirst(long first) {
      this.first = first;
    }

    /**
     * Get the second element.
     *
     * @return The second element
     */
    public long getSecond() {
      return second;
    }

    /**
     * Set the second element.
     *
     * @param second The second element
     */
    public void setSecond(long second) {
      this.second = second;
    }
  }
}
