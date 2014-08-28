package org.hf.mls.cf.hadoop;

import org.hf.mls.common.JobOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * Created by He Fan on 2014/7/8.
 */
public class ExtractTmpJob {
    public static Job init(String[] args) throws IOException {
        System.out.println("Extract Tmp job started!");
        Configuration conf = new Configuration();

        Job job = new Job(conf, "CF_Extract Dic_" + args[2]);
        job.setJarByClass(ExtractTmpJob.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (JobOptions.COMPRESS.equals(args[3])) {
            FileOutputFormat.setCompressOutput(job, true);
            FileOutputFormat.setOutputCompressorClass(job, JobOptions.COMPRESS_CLASS);
            SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        }

        job.setMapperClass(ExtractMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    public static class ExtractMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            int length = 0;

            String[] fields = value.toString().split("\t");
            String userId = fields[0];

            String mid_1 = fields[1].split("\\[")[1];
            String[] mid_2 = mid_1.split(",");
            length = mid_2.length;

            for (int i = 0; i < length; i++) {
                String itemId = mid_2[i].split(":")[0];
                context.write(new Text(userId + "," + itemId), new Text(""));
            }

        }
    }

    public static class ReverseReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key, key);
        }
    }
}
