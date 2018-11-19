import java.lang.*;
import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

/**
 * A Triangle Type Partition graph triangle counting algorithm implementation
 * for Hadoop by nathanchrs and rayandrews.
 *
 * Implements the TTP algorithm proposed in the 2013 paper of Ha-Myung Park and
 * Chin-Wan Chung of KAIST (An Efficient MapReduce Algorithm for Counting
 * Triangles in a Very Large Graph, CIKM '13)
 * http://islab.kaist.ac.kr/chungcw/InterConfPapers/km0805-ha-myung.pdf or
 * http://dx.doi.org/10.1145/2505515.2505563
 *
 * For each partition, we use the Compact-Forward triangle counting algorithm as
 * outlined in Matthieu Latapy's 2008 paper (Main-memory triangle computations
 * for very large (sparse (power-law)) graphs, Theoretical Computer Science,
 * 407(1):458-473)
 * http://complexnetworks.fr/wp-content/uploads/2011/01/triangles.pdf
 */
public class TriangleTypePartition extends Configured implements Tool {

  public static final long DEFAULT_PARTITION_COUNT = 64;
  public static final String PARTITION_COUNT_CONFIG_KEY = "partitionCount";

  public static final int ESTIMATED_VERTEX_COUNT_PER_REDUCE = 200000;
  public static final int ESTIMATED_VERTEX_DEGREE_PER_REDUCE = 100;

  public static final Text TYPE_1_TRIANGLE_COUNT_KEY = new Text("A");
  public static final Text TYPE_2_OR_3_TRIANGLE_COUNT_KEY = new Text("B");
  public static final Text RESULT_KEY = new Text("TriangleCount");

  public static class MapperOne extends Mapper<LongWritable, Text, LongPair, NullWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] valueStrings = value.toString().split("\\s+");
      if (valueStrings.length > 1) {
        long vertex1 = Long.parseLong(valueStrings[0]);
        long vertex2 = Long.parseLong(valueStrings[1]);

        if (vertex1 < vertex2) {
          context.write(new LongPair(vertex1, vertex2), NullWritable.get());
        } else {
          context.write(new LongPair(vertex2, vertex1), NullWritable.get());
        }
      }
    }
  }

  public static class ReducerOne extends Reducer<LongPair, NullWritable, Text, NullWritable> {
    public void reduce(LongPair key, Iterable<NullWritable> values, Context context)
        throws IOException, InterruptedException {
      context.write(new Text(String.valueOf(key.first) + " " + String.valueOf(key.second)), NullWritable.get());
    }
  }

  public static class MapperTwo extends Mapper<LongWritable, Text, Text, LongPair> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      long p = conf.getLong(PARTITION_COUNT_CONFIG_KEY, DEFAULT_PARTITION_COUNT);

      String[] valueStrings = value.toString().split("\\s+");
      if (valueStrings.length > 1) {
        long vertex1 = Long.parseLong(valueStrings[0]);
        long vertex2 = Long.parseLong(valueStrings[1]);
        long vertexPartition1 = vertex1 % p;
        long vertexPartition2 = vertex2 % p;

        for (long a = 0; a < p - 1; a++) {
          for (long b = a + 1; b < p; b++) {
            if (((vertexPartition1 == a) && (vertexPartition2 == b))
                || ((vertexPartition1 == b) && (vertexPartition2 == a))
                || ((vertexPartition1 == a) && (vertexPartition2 == a))
                || ((vertexPartition1 == b) && (vertexPartition2 == b))) {
              context.write(new Text(String.valueOf(a) + "," + String.valueOf(b)), new LongPair(vertex1, vertex2));
            }
          }
        }

        if (vertexPartition1 != vertexPartition2) {
          for (long a = 0; a < p - 2; a++) {
            for (long b = a + 1; b < p - 1; b++) {
              for (long c = b + 1; c < p; c++) {
                if (((vertexPartition1 == a) && (vertexPartition2 == a))
                    || ((vertexPartition1 == a) && (vertexPartition2 == b))
                    || ((vertexPartition1 == a) && (vertexPartition2 == c))
                    || ((vertexPartition1 == b) && (vertexPartition2 == a))
                    || ((vertexPartition1 == b) && (vertexPartition2 == b))
                    || ((vertexPartition1 == b) && (vertexPartition2 == c))
                    || ((vertexPartition1 == c) && (vertexPartition2 == a))
                    || ((vertexPartition1 == c) && (vertexPartition2 == b))
                    || ((vertexPartition1 == c) && (vertexPartition2 == c))) {
                  context.write(new Text(String.valueOf(a) + "," + String.valueOf(b) + "," + String.valueOf(c)),
                      new LongPair(vertex1, vertex2));
                }
              }
            }
          }
        }
      }
    }
  }

  public static class ReducerTwo extends Reducer<Text, LongPair, Text, LongWritable> {
    final Map<Long, List<Long>> adjacencyList = new HashMap(ESTIMATED_VERTEX_COUNT_PER_REDUCE);
    List<Long> vertices = new ArrayList(ESTIMATED_VERTEX_COUNT_PER_REDUCE);

    // Comparator for ordering vertices by degree, then by index in descending order
    public int compareVertices(long v1, long v2) {
      return Long.compare((((long) this.adjacencyList.get(v2).size()) << 34) + v2,
          (((long) this.adjacencyList.get(v1).size()) << 34) + v1);
    }

    public void reduce(Text key, Iterable<LongPair> values, Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      long p = conf.getLong(PARTITION_COUNT_CONFIG_KEY, DEFAULT_PARTITION_COUNT);

      // Add edges to the graph - assumes edges are unique
      for (LongPair edge : values) {

        if (!adjacencyList.containsKey(edge.first)) {
          adjacencyList.put(edge.first, new ArrayList(ESTIMATED_VERTEX_DEGREE_PER_REDUCE));
          vertices.add(edge.first);
        }
        if (!adjacencyList.containsKey(edge.second)) {
          adjacencyList.put(edge.second, new ArrayList(ESTIMATED_VERTEX_DEGREE_PER_REDUCE));
          vertices.add(edge.second);
        }

        adjacencyList.get(edge.first).add(edge.second);
        adjacencyList.get(edge.second).add(edge.first);
      }

      // Sort vertices and adjacency list
      Collections.sort(vertices, new Comparator<Long>() {
        @Override
        public int compare(Long v1, Long v2) {
          return compareVertices(v1, v2);
        }
      });
      for (Long vertex : vertices) {
        Collections.sort(adjacencyList.get(vertex), new Comparator<Long>() {
          @Override
          public int compare(Long v1, Long v2) {
            return compareVertices(v1, v2);
          }
        });
      }

      long type1TriangleCount = 0;
      long type2Or3TriangleCount = 0;

      for (Long v1 : vertices) {
        List<Long> v1Neighbors = adjacencyList.get(v1);

        for (Long v2 : v1Neighbors) {
          if (compareVertices(v1, v2) < 0) {
            List<Long> v2Neighbors = adjacencyList.get(v2);

            // Find intersections between v1Neighbors and v2Neighbors
            // using a method similar to merge sort's merge
            int i1 = 0;
            int i2 = 0;
            while ((i1 < v1Neighbors.size()) && (i2 < v2Neighbors.size())
                && (compareVertices(v1Neighbors.get(i1), v1) < 0) && (compareVertices(v2Neighbors.get(i2), v1) < 0)) {
              if (compareVertices(v2Neighbors.get(i2), v1Neighbors.get(i1)) < 0) {
                i2++;
              } else if (compareVertices(v2Neighbors.get(i2), v1Neighbors.get(i1)) > 0) {
                i1++;
              } else {
                // Found an intersection (triangle)
                long v1Partition = v1 % p;
                long v2Partition = v2 % p;
                long v3Partition = v1Neighbors.get(i1) % p;
                if ((v1Partition == v2Partition) && (v2Partition == v3Partition)) {
                  type1TriangleCount++;
                } else {
                  type2Or3TriangleCount++;
                }
                i1++;
                i2++;
              }
            }
          }
        }
      }

      context.write(TYPE_1_TRIANGLE_COUNT_KEY, new LongWritable(type1TriangleCount));
      context.write(TYPE_2_OR_3_TRIANGLE_COUNT_KEY, new LongWritable(type2Or3TriangleCount));

      adjacencyList.clear();
      vertices.clear();
    }
  }

  public static class MapperThree extends Mapper<LongWritable, Text, Text, LongWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] valueStrings = value.toString().split("\\s+");
      if (valueStrings.length > 1) {
        context.write(new Text(valueStrings[0].trim()), new LongWritable(Long.parseLong(valueStrings[1])));
      }
    }
  }

  public static class ReducerThree extends Reducer<Text, LongWritable, Text, LongWritable> {
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      long p = conf.getLong(PARTITION_COUNT_CONFIG_KEY, DEFAULT_PARTITION_COUNT);

      long sum = 0;
      for (LongWritable value : values) {
        sum += value.get();
      }

      if (key.toString().equals(TYPE_1_TRIANGLE_COUNT_KEY)) {
        sum /= (p - 1);
      }

      context.write(key, new LongWritable(sum));
    }
  }

  public int run(String[] args) throws Exception {

    /* Set up configuration */

    String inputPath = args[0];
    String outputPath = args[1];
    long partitionCount = Long.parseLong(args[2]);
    getConf().setLong(PARTITION_COUNT_CONFIG_KEY, partitionCount);

    /* Job 1: Remove duplicate edges */

    Job jobOne = new Job(getConf());
    jobOne.setJobName("first-mapreduce");

    jobOne.setMapOutputKeyClass(LongPair.class);
    jobOne.setMapOutputValueClass(NullWritable.class);
    jobOne.setOutputKeyClass(Text.class);
    jobOne.setOutputValueClass(NullWritable.class);

    jobOne.setJarByClass(TriangleTypePartition.class);
    jobOne.setMapperClass(MapperOne.class);
    jobOne.setReducerClass(ReducerOne.class);

    TextInputFormat.addInputPath(jobOne, new Path(inputPath));
    TextOutputFormat.setOutputPath(jobOne, new Path("/user/wennyyustalim/temp/first-mapreduce"));

    /* Job 2: Triangle Type Partition algorithm */

    Job jobTwo = new Job(getConf());
    jobTwo.setJobName("second-mapreduce");

    jobTwo.setMapOutputKeyClass(Text.class);
    jobTwo.setMapOutputValueClass(LongPair.class);
    jobTwo.setOutputKeyClass(Text.class);
    jobTwo.setOutputValueClass(LongWritable.class);

    jobTwo.setJarByClass(TriangleTypePartition.class);
    jobTwo.setMapperClass(MapperTwo.class);
    jobTwo.setReducerClass(ReducerTwo.class);

    TextInputFormat.addInputPath(jobTwo, new Path("/user/wennyyustalim/temp/first-mapreduce"));
    TextOutputFormat.setOutputPath(jobTwo, new Path("/user/wennyyustalim/temp/second-mapreduce"));

    /* Job 3: Sum triangle counts */

    Job jobThree = new Job(getConf());
    jobThree.setJobName("third-mapreduce");
    jobThree.setNumReduceTasks(1);

    jobThree.setMapOutputKeyClass(Text.class);
    jobThree.setMapOutputValueClass(LongWritable.class);
    jobThree.setOutputKeyClass(Text.class);
    jobThree.setOutputValueClass(LongWritable.class);

    jobThree.setJarByClass(TriangleTypePartition.class);
    jobThree.setMapperClass(MapperThree.class);
    jobThree.setReducerClass(ReducerThree.class);

    TextInputFormat.addInputPath(jobThree, new Path("/user/wennyyustalim/temp/second-mapreduce"));
    TextOutputFormat.setOutputPath(jobThree, new Path(outputPath));

    /* Execute jobs */

    long startTime = System.nanoTime();

    int ret = jobOne.waitForCompletion(true) ? 0 : 1;
    if (ret == 0)
      ret = jobTwo.waitForCompletion(true) ? 0 : 1;
    if (ret == 0)
      ret = jobThree.waitForCompletion(true) ? 0 : 1;

    long endTime = System.nanoTime();

    long executionWallClockTime = endTime - startTime;
    System.out.println("Execution wall clock time: " + (executionWallClockTime / 1000000000) + " seconds");

    return ret;
  }

  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new Configuration(), new TriangleTypePartition(), args);
    System.exit(ret);
  }
}
