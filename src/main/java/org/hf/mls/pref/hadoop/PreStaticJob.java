package org.hf.mls.pref.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
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
 * Created by He Fan on 2014/6/3.
 */
public class PreStaticJob extends Configured implements Tool {

    public static long runJob(String[] args) throws Exception {
        return ToolRunner.run(new Configuration(), new PreStaticJob(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setStrings("BatchId", args[2]);
        conf.set("split", args[3]);

        Job job = new Job(conf, "PREF_Pre Statistic_" + args[2]);
        job.setJarByClass(PreStaticJob.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileOutputFormat.setCompressOutput(job, true);

        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        job.setMapperClass(PreStaticMapper.class);
        job.setReducerClass(PreStaticReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        boolean success = job.waitForCompletion(true);
        if (success) {
            return 1;
        } else {
            return 0;
        }
    }

    public static class PreStaticMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration map_conf = context.getConfiguration();
            String splitChar = map_conf.get("split", "\\x01");

            String[] fields = value.toString().split(splitChar);
            String batchUserId = fields[0].replace(splitChar, " ").trim();
            String item_id = fields[1].replace(splitChar, " ").trim();
            String prefs_value = fields[2].replace(splitChar, " ").trim();

            prefs_value = String.format("%.2f", Double.valueOf(prefs_value));

            context.write(new Text("I" + item_id), new Text(batchUserId));
            context.write(new Text("P" + prefs_value), new Text(batchUserId));
        }
    }

    public static class PreStaticReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int n = 0;

            Configuration map_conf = context.getConfiguration();
            String batchId = map_conf.getStrings("BatchId", "")[0];

            String target = key.toString();

            String newKey = "";
            String newValue = "";

            for (Text value : values) {
                n++;
            }
            if ("I".equals(target.substring(0, 1))) {
                newKey = batchId + "_01";
            } else if ("P".equals(target.substring(0, 1))) {
                newKey = batchId + "_02";
            }

            newValue = target.substring(1) + ":" + String.valueOf(n);
            context.write(new Text(newKey), new Text(newValue));
        }
    }
}