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

public class TriangleCount extends Configured implements Tool {

  public static class FirstMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] str = value.toString().split("\\s+");
      if (str.length > 1) {
        long node1 = Long.parseLong(str[0]);
        long node2 = Long.parseLong(str[1]);

        if (node1 < node2)
          context.write(new LongWritable(node1), new LongWritable(node2));
        else
          context.write(new LongWritable(node2), new LongWritable(node1));
      }
    }
  }

  public static class FirstReducer extends Reducer<LongWritable, LongWritable, Text, Text> {
    Text rKey = new Text();
    Text rValue = new Text();
    private LongWritable dollar = new LongWritable(-1);

    public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<LongWritable> vs = values.iterator();

      while (vs.hasNext()) {

        long e = vs.next().get();
        rKey.set(key.toString() + ',' + Long.toString(e));
        rValue.set(dollar.toString());
        context.write(rKey, rValue);

        if (vs.hasNext()) {
          long f = vs.next().get();

          rKey.set(key.toString() + ',' + Long.toString(f));
          rValue.set(dollar.toString());
          context.write(rKey, rValue);

          if (e != f) {
            rKey.set(Long.toString(e) + ',' + Long.toString(f));
            rValue.set(key.toString());
            context.write(rKey, rValue);
          }
        }
      }
    }
  }

  public static class MapperTextLongWritable extends Mapper<LongWritable, Text, Text, LongWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] str = value.toString().split("\\s+");
      if (str.length > 1) {
        context.write(new Text(str[0]), new LongWritable(Long.parseLong(str[1])));
      }
    }
  }

  public static class SecondReducer extends Reducer<Text, LongWritable, LongWritable, LongWritable> {
    private long dollar = -1;

    int count = 0;
    boolean isDollarExist = false;

    public void cleanup(Context context) throws IOException, InterruptedException {
      if (isDollarExist)
        context.write(new LongWritable(0), new LongWritable(count));
    }

    public void reduce(Text key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<LongWritable> vs = values.iterator();

      while (vs.hasNext()) {
        long e = vs.next().get();

        if (e != dollar) {
          count++;
        }

        isDollarExist |= e == dollar;
      }
    }
  }

  public static class ThirdReducer extends Reducer<Text, LongWritable, LongWritable, NullWritable> {
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
      long sum = 0;
      Iterator<LongWritable> vs = values.iterator();
      while (vs.hasNext()) {
        sum += vs.next().get();
      }
      context.write(new LongWritable(sum), NullWritable.get());
    }
  }

  public int run(String[] args) throws Exception {
    /**
     * Job One
     */
    Job jobOne = new Job(getConf());
    jobOne.setJobName("mapreduce-one");

    jobOne.setMapOutputKeyClass(LongWritable.class);
    jobOne.setMapOutputValueClass(LongWritable.class);

    jobOne.setOutputKeyClass(Text.class);
    jobOne.setOutputValueClass(Text.class);

    jobOne.setJarByClass(TriangleCount.class);
    jobOne.setMapperClass(FirstMapper.class);
    jobOne.setReducerClass(FirstReducer.class);

    FileInputFormat.addInputPath(jobOne, new Path(args[0]));
    FileOutputFormat.setOutputPath(jobOne, new Path("/user/rayandrew/temp/mapreduce-one"));

    /**
     * Job Two
     */
    Job jobTwo = new Job(getConf());
    jobTwo.setJobName("mapreduce-two");

    jobTwo.setMapOutputKeyClass(Text.class);
    jobTwo.setMapOutputValueClass(LongWritable.class);

    jobTwo.setOutputKeyClass(LongWritable.class);
    jobTwo.setOutputValueClass(LongWritable.class);

    jobTwo.setJarByClass(TriangleCount.class);
    jobTwo.setMapperClass(MapperTextLongWritable.class);
    jobTwo.setReducerClass(SecondReducer.class);

    FileInputFormat.addInputPath(jobTwo, new Path("/user/rayandrew/temp/mapreduce-one"));
    FileOutputFormat.setOutputPath(jobTwo, new Path("/user/rayandrew/temp/mapreduce-two"));

    /**
     * Job Three
     */
    Job jobThree = new Job(getConf());
    jobThree.setJobName("mapreduce-three");
    jobThree.setNumReduceTasks(1);

    jobThree.setMapOutputKeyClass(Text.class);
    jobThree.setMapOutputValueClass(LongWritable.class);

    jobThree.setOutputKeyClass(LongWritable.class);
    jobThree.setOutputValueClass(NullWritable.class);

    jobThree.setJarByClass(TriangleCount.class);
    jobThree.setMapperClass(MapperTextLongWritable.class);
    jobThree.setReducerClass(ThirdReducer.class);

    FileInputFormat.addInputPath(jobThree, new Path("/user/rayandrew/temp/mapreduce-two"));
    FileOutputFormat.setOutputPath(jobThree, new Path(args[1]));

    int ret = jobOne.waitForCompletion(true) ? 0 : 1;
    if (ret == 0)
      ret = jobTwo.waitForCompletion(true) ? 0 : 1;
    if (ret == 0)
      ret = jobThree.waitForCompletion(true) ? 0 : 1;

    return ret;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new TriangleCount(), args);
    System.exit(res);
  }
}