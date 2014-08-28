package org.hf.mls.pref.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by He Fan on 2014/6/13.
 */
public class PreHbaseJob extends Configured implements Tool {

    public static long runJob(String[] args) throws Exception {
        return ToolRunner.run(new Configuration(), new PreHbaseJob(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("split",args[3]);

        Job job = new Job(conf, "PREF_PreHbase_" + args[2]);
        job.setJarByClass(PreHbaseJob.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(PreHbaeMapper.class);
        job.setReducerClass(PreHbaseReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        boolean success = job.waitForCompletion(true);
        if (success) {
            return 1;
        } else {
            return 0;
        }
    }

    public static class PreHbaeMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration map_conf = context.getConfiguration();
            String splitChar = map_conf.get("split","\\x01");

            String[] fields = value.toString().split(splitChar);
            String batchUserId = fields[0].replace(splitChar, " ").trim();
            String item_id = fields[1].replace(splitChar, " ").trim();

            String newKey = batchUserId;
            String newValue = item_id + ":" + fields[2].replace(splitChar, " ").trim();

            context.write(new Text(newKey), new Text(newValue));
        }
    }

    public static class PreHbaseReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String newValue = "";

            for (Text value : values) {
                if ("".equals(newValue)) {
                    newValue = value.toString();
                } else {
                    newValue += "," + value.toString();
                }
            }
            context.write(key, new Text(newValue));
        }
    }
}