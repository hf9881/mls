package org.hf.mls.cf.hadoop;

import org.hf.mls.common.JobOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * Created by He Fan on 2014/4/9.
 */
public class EvaluateJob {

    public static Job init(String[] args) throws IOException {
        System.out.println("Evaluate job started!");
        Configuration conf = new Configuration();
        conf.setStrings("BatchId", args[2]);

        Job job = new Job(conf, "CF_Evaluate_" + args[2]);
        job.setJarByClass(EvaluateJob.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(SequenceFileInputFormat.class);
        //job.setOutputFormatClass(SequenceFileOutputFormat.class);

        if (JobOptions.COMPRESS.equals(args[3])) {
            FileOutputFormat.setCompressOutput(job, true);
            FileOutputFormat.setOutputCompressorClass(job, JobOptions.COMPRESS_CLASS);
            SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        }

        job.setMapperClass(EvaluateMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job;
    }

    public static class EvaluateMapper extends Mapper<Writable, Writable, Text, Text> {

        @Override
        public void map(Writable key, Writable value, Context context) throws IOException, InterruptedException {
            Configuration mapConf = context.getConfiguration();
            String batchId = mapConf.getStrings("BatchId", "")[0];

            StringBuilder newKey = new StringBuilder(batchId).append("_").append(key.toString());
            String[] arguments = value.toString().split(",");
            String Nrs = arguments[0];
            String Nr = arguments[1];
            String Ns = arguments[2];

            double nrs = Double.parseDouble(Nrs);
            double precision = 0;
            double recall = 0;
            double f1 = 0;

            if (0 != nrs) {
                precision = nrs / Double.parseDouble(Ns);
                recall = nrs / Double.parseDouble(Nr);
                f1 = 2 * precision * recall / (precision + recall);
            }
            StringBuilder newValue = new StringBuilder(String.valueOf(precision))
                    .append(",")
                    .append(String.valueOf(recall))
                    .append(",")
                    .append(String.valueOf(f1));
            context.write(new Text(newKey.append(",").append(newValue).toString()), new Text(""));
        }

    }
}
