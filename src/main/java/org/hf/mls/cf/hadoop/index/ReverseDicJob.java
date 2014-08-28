package org.hf.mls.cf.hadoop.index;

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
 * Created by He Fan on 2014/7/7.
 */
public class ReverseDicJob {
    public static Job init(String[] args) throws IOException {
        String tag = "";
        if ("0".equals(args[3])) {
            tag = "User";
        } else {
            tag = "Item";
        }
        System.out.println("Reverse " + tag + " dic job started!");
        Configuration conf = new Configuration();

        Job job = new Job(conf, "CF_Reverse " + tag + " Dic_" + args[2]);
        job.setJarByClass(CreateDicJob.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (JobOptions.COMPRESS.equals(args[4])) {
            FileOutputFormat.setCompressOutput(job, true);
            FileOutputFormat.setOutputCompressorClass(job, JobOptions.COMPRESS_CLASS);
            SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        }

        job.setMapperClass(ReverseMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    public static class ReverseMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //the value format is:
            // User,_Index
            //or
            // ,Item,_Index
            //reverse to:
            // Index,_User
            //or
            // ,Index,_Item
            String[] items = value.toString().split(",");
            String index = "";
            String word = "";
            String newKey = "";
            index = items[items.length - 1].replace("\t", "").trim();
            word = items[items.length - 2].replace("\t", "").trim();
            newKey = index.substring(1) + ",_" + word;
            if (3 == items.length) {
                newKey = "," + newKey;
            }
            context.write(new Text(newKey), new Text(""));
        }
    }
}
