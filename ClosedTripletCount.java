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
            for (LongWritable u : values) {
                // rKey.set("$");
                // rValue.set(key.toString() + ',' + u.toString());
                context.write(new Text("$"), new Text(key.toString() + ',' + u.toString()));

                for (LongWritable w : values) {
                    if (u != w) {
                        // rKey.set(key.toString());
                        // rValue.set(u.toString() + ',' + w.toString());
                        context.write(new Text(key.toString()), new Text(u.toString() + ',' + w.toString()));
                    }
                }
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

        int ret = jobOne.waitForCompletion(true) ? 0 : 1;

        return ret;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ClosedTripletCount(), args);
        System.exit(res);
    }
}