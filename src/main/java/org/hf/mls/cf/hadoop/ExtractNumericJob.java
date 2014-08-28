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
public class ExtractNumericJob {

    public static Job init(String[] args) throws IOException {
        System.out.println("Reduction Numeric job started!");
        Configuration conf = new Configuration();

        Job job = new Job(conf, "CF_Reduction Numeric_" + args[2]);
        job.setJarByClass(ExtractNumericJob.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (JobOptions.COMPRESS.equals(args[3])) {
            FileOutputFormat.setCompressOutput(job, true);
            FileOutputFormat.setOutputCompressorClass(job, JobOptions.COMPRESS_CLASS);
            SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        }

        job.setMapperClass(DicMapper.class);
        job.setReducerClass(DicReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    public static class DicMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] items = value.toString().split(",");
            context.write(new Text(items[0]), new Text(items[1]));
        }
    }

    public static class DicReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuilder newKey = new StringBuilder(key.toString());
            StringBuilder newValue = new StringBuilder("");
            for (Text v : values) {
                if (0 != newValue.length()) {
                    newValue.append(",");
                }
                newValue.append(v.toString());
            }
            context.write(new Text(newKey.toString()), new Text(newValue.toString()));
        }
    }
}
