import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class ClosedTripletCount extends Configured implements Tool {
    public static class FirstMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
        public void map(LongWritable k, Text text, Context context) throws IOException, InterruptedException {
            String[] pair = text.toString().split("\\s+");
            if (pair.length > 1) { // if edge is valid
                long u = Long.parseLong(pair[0]);
                long v = Long.parseLong(pair[1]);

                if (u < v) {
                    context.write(new LongWritable(u), new LongWritable(v));
                } else {
                    context.write(new LongWritable(v), new LongWritable(u));
                }
            }
        }
    }

    public static class FirstReducer extends Reducer<LongWritable, LongWritable, Text, Text> {
        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            ArrayList<Long> valuesCopy = new ArrayList<Long>();
            for (LongWritable u : values) {
                valuesCopy.add(u.get());
                context.write(new Text(key.toString() + ',' + u.toString()), new Text("$"));
            }
            for (int u = 0; u < valuesCopy.size(); ++u) {
                for (int w = 0; w < valuesCopy.size(); ++w) {
                    if (valuesCopy.get(u) < valuesCopy.get(w)) {
                        context.write(new Text(valuesCopy.get(u).toString() + ',' + valuesCopy.get(w).toString()), new Text(key.toString()));
                    }
                }
            }
        }
    }

    public static class SecondMapper extends Mapper<Text, Text, Text, Text> {
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class SecondReducer extends Reducer<Text, Text, LongWritable, LongWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            LinkedHashSet<String> valueSet = new LinkedHashSet<String>();
            for (Text value: values) {
                valueSet.add(value.toString());
            }
            long count = 0;
            boolean valid = false;
            for (String value: valueSet) {
                if (!value.equals("$")) {
                    ++count;
                } else {
                    valid = true;
                }
            }
            if (valid) {
                context.write(new LongWritable(0), new LongWritable(count));
            }
        }
    }

    public int run(String[] args) throws Exception {
        Job jobOne = new Job(getConf());
        jobOne.setJobName("first-mapreduce");

        jobOne.setMapOutputKeyClass(LongWritable.class);
        jobOne.setMapOutputValueClass(LongWritable.class);

        jobOne.setOutputKeyClass(Text.class);
        jobOne.setOutputValueClass(Text.class);

        jobOne.setJarByClass(ClosedTripletCount.class);
        jobOne.setMapperClass(FirstMapper.class);
        jobOne.setReducerClass(FirstReducer.class);

        FileInputFormat.addInputPath(jobOne, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobOne, new Path("/user/wennyyustalim/temp/first-mapreduce"));

        Job jobTwo = new Job(getConf());
        jobTwo.setJobName("mapreduce-two");

        jobTwo.setMapOutputKeyClass(Text.class);
        jobTwo.setMapOutputValueClass(LongWritable.class);

        jobTwo.setOutputKeyClass(LongWritable.class);
        jobTwo.setOutputValueClass(LongWritable.class);

        jobTwo.setJarByClass(ClosedTripletCount.class);
        jobTwo.setMapperClass(SecondMapper.class);
        jobTwo.setReducerClass(SecondReducer.class);

        FileInputFormat.addInputPath(jobTwo, new Path("/user/wennyyustalim/temp/first-mapreduce"));
        FileOutputFormat.setOutputPath(jobTwo, new Path("/user/wennyyustalimn/temp/second-mapreduce"));

        int ret = jobOne.waitForCompletion(true) ? 0 : 1;

        return ret;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ClosedTripletCount(), args);
        System.exit(res);
    }
}