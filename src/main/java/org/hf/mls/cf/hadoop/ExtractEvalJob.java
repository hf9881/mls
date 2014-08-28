package org.hf.mls.cf.hadoop;

import org.hf.mls.common.JobOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * Created by He Fan on 2014/5/30.
 */
public class ExtractEvalJob {

    public static Job init(String[] args) throws IOException {
        System.out.println("Extract cf evaluation output data set job started!");
        Configuration conf = new Configuration();
        conf.setStrings("BatchId", args[2]);

        Job job = new Job(conf, "CF_Extract eval_" + args[4]);
        job.setJarByClass(ExtractEvalJob.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        if (JobOptions.COMPRESS.equals(args[3])) {
            FileOutputFormat.setCompressOutput(job, true);
            FileOutputFormat.setOutputCompressorClass(job, JobOptions.COMPRESS_CLASS);
            SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        }
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(ExtractEvalMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job;
    }

    public static class ExtractEvalMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration map_conf = context.getConfiguration();
            String batchId = map_conf.getStrings("BatchId", "")[0];

            StringBuilder newKey = new StringBuilder("");
            StringBuilder newValue = new StringBuilder("");
            int length = 0;

            String[] fields = value.toString().split("\t");
            String userId = fields[0];

            String mid_1 = fields[1].split("\\[")[1];
            String[] mid_2 = mid_1.split(",");
            length = mid_2.length;

            for (int i = 0; i < length; i++) {
                String itemId = mid_2[i].split(":")[0];
                if (0 != newValue.length()) {
                    newValue.append(",");
                }
                newValue.append(itemId);
            }

            if ("Nul".equals(batchId)) {
                newKey.append(userId);
                newValue = new StringBuilder("").append("Ns").append(String.valueOf(length)).append(",").append(newValue);
            } else {
                newKey.append(batchId).append("_").append(userId);
            }

            context.write(new Text(newKey.toString()), new Text(newValue.toString()));
        }
    }
}