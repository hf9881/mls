package org.hf.mls.ar.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by He Fan on 2014/4/10.
 */
public class CountJob extends Configured implements Tool {

    public static long getCount(String[] args) throws Exception {
        return ToolRunner.run(new Configuration(), new CountJob(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //conf.setLong("N", 0);

        Job job = new Job(conf, "AR_Count_" + args[2]);
        job.setJarByClass(CountJob.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapperClass(CountMapper.class);

        job.setNumReduceTasks(0);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        boolean success = job.waitForCompletion(true);
        if (success) {
            return (int) job.getCounters().findCounter("Counter", "line").getValue();
        } else {
            System.out.println("Count job failed!");
            return 0;
        }
    }

    public static class CountMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            context.getCounter("Counter", "line").increment(1);
        }
    }
}
