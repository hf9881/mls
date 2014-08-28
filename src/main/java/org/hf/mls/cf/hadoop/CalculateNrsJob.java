package org.hf.mls.cf.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.hf.mls.common.JobOptions;

import java.io.IOException;

/**
 * Created by He Fan on 2014/5/27.
 */
public class CalculateNrsJob {

    public static Job init(String[] args) throws IOException {
        System.out.println("Calculate Nrs job started!");
        Configuration conf = new Configuration();

        Job job = new Job(conf, "CF_Calculate Nrs_" + args[4]);
        job.setJarByClass(CalculateNrsJob.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        if (JobOptions.COMPRESS.equals(args[3])) {
            FileOutputFormat.setCompressOutput(job, true);
            FileOutputFormat.setOutputCompressorClass(job, JobOptions.COMPRESS_CLASS);
            SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        }

        FileInputFormat.setInputPaths(job, new Path(args[0]), new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setMapperClass(CalculateMapper.class);
        job.setReducerClass(CalculateReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job;
    }

    public static class CalculateMapper extends Mapper<Writable, Writable, Text, Text> {
        @Override
        public void map(Writable key, Writable value, Context context) throws IOException, InterruptedException {
            context.write(new Text(key.toString()), new Text(value.toString()));
        }
    }

    public static class CalculateReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder newValue = new StringBuilder("");
            String[] tmp;
            String[] trainItems = null;
            String[] testItems = null;
            int Nrs = 0;
            String Nr = "";
            String Ns = "";

            for (Text value : values) {
                tmp = value.toString().split(",");
                if ("Ns".equals(tmp[0].substring(0, 2))) {
                    trainItems = tmp;
                    Ns = tmp[0].split("Ns")[1];
                } else if ("Nr".equals(tmp[0].substring(0, 2))) {
                    testItems = tmp;
                    Nr = tmp[0].split("Nr")[1];
                }
            }
            if (null != testItems && null != trainItems) {
                for (int i = 1; i < trainItems.length; i++) {
                    for (int j = 1; j < testItems.length; j++) {
                        if (Integer.parseInt(trainItems[i]) == Integer.parseInt(testItems[j])) {
                            Nrs++;
                            break;
                        }
                    }
                }
                newValue.append(String.valueOf(Nrs)).append(",").append(Nr).append(",").append(Ns);
                context.write(key, new Text(newValue.toString()));
            }

        }
    }
}