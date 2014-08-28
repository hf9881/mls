package org.hf.mls.ar.hadoop;

import org.hf.mls.common.JobOptions;
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
 * Created by He Fan on 2014/5/27.
 */
public class PreStaticJob extends Configured implements Tool {

    public static long staticData(String[] args) throws Exception {
        return ToolRunner.run(new Configuration(), new PreStaticJob(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "AR_Pre statistic_" + args[3]);
        job.setJarByClass(PreStaticJob.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        if (JobOptions.COMPRESS.equals(args[2])) {
            FileOutputFormat.setCompressOutput(job, true);
            FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
            SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        }

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
            //Sample : b01_016006-006013,2.34484179106882
            String lifValue = value.toString().split(",")[1];
            String newKey = String.format("%.1f", Double.valueOf(lifValue));
            String newValue = "1";

            context.write(new Text(newKey), new Text(newValue));
        }
    }

    public static class PreStaticReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int n = 0;

            String newKey = "01";
            StringBuilder newValue = new StringBuilder(key.toString());

            for (Text value : values) {
                n++;
            }
            newValue.append(":").append(String.valueOf(n));
            context.write(new Text(newKey), new Text(newValue.toString()));
        }
    }
}