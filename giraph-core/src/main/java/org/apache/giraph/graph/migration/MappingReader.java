package org.apache.giraph.graph.migration;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.apache.giraph.conf.GiraphConfigurationSettable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by ravikant on 1/2/17.
 */

//TODO: return a table of mapping
public class MappingReader {
  private static Logger LOG = Logger.getLogger(MappingReader.class);

  public static ArrayList<MappingRow> readFile(ImmutableClassesGiraphConfiguration conf) throws IOException {

    String cvsSplitBy = ",";
    ArrayList<MappingRow> records = new ArrayList<>();
    //read this as custom argument
    Path pt = new Path(conf.getPartitionStatsFile());
    FileSystem fs = FileSystem.get(new Configuration());
    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
    String line;

    line = br.readLine();
    //input format superstep,partition,time
    while (line != null) {

      String[] entry = line.split(cvsSplitBy);
      MappingRow mr = new MappingRow(Integer.parseInt(entry[0]), Integer.parseInt(entry[1]), Float.parseFloat(entry[2]));
      records.add(mr);
      LOG.debug("TEST,ReadMappingFromHDFS.readFile," + line);
      line = br.readLine();
    }


    return records;
  }
}