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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Created by He Fan on 2014/5/27.
 */
public class StaticEvalJob {

    public static Job init(String[] args) throws IOException {
        System.out.println("PreStatistic job started!");
        Configuration conf = new Configuration();
        conf.setStrings("BatchId", args[2]);

        Job job = new Job(conf, "CF_Pre statistic_" + args[2]);
        job.setJarByClass(StaticEvalJob.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        if (JobOptions.COMPRESS.equals(args[3])) {
            FileOutputFormat.setCompressOutput(job, true);
            FileOutputFormat.setOutputCompressorClass(job, JobOptions.COMPRESS_CLASS);
            SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        }

        job.setMapperClass(PreStaticMapper.class);
        job.setReducerClass(PreStaticReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job;
    }

    public static class PreStaticMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration map_conf = context.getConfiguration();
            String batchId = map_conf.getStrings("BatchId", "")[0];

            String newKey = batchId + "_01";

            String[] v = value.toString().split(",");
            StringBuilder newValue = new StringBuilder("");
            //String precision = v[1];
            //String recall = v[2];
            //String f1 = v[3];
            newValue.append(v[1]).append(",").append(v[2]).append(",").append(v[3]);

            context.write(new Text(newKey), new Text(newValue.toString()));
        }
    }

    public static class PreStaticReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Configuration map_conf = context.getConfiguration();
            String batchId = map_conf.getStrings("BatchId", "")[0];
            double n = 0;
            StringBuilder quotaPairs = new StringBuilder("");
            String newKey = "";
            StringBuilder newValue = new StringBuilder("");
            ArrayList<String> fList = new ArrayList();
            HashMap<String, Integer> fMap = new HashMap();
            HashSet<String> quotaSet = new HashSet<String>();
            double sp = 0;
            double sr = 0;
            double sf = 0;

            for (Text value : values) {
                String[] items = value.toString().split(",");
                double p = Double.valueOf(items[0]);
                sp += p;
                double r = Double.valueOf(items[1]);
                sr += r;
                double f = Double.valueOf(items[2]);
                sf += f;
                fList.add(String.format("%.1f", f));
                n++;
                quotaSet.add(new StringBuilder("").append(String.format("%.4f", p)).append(":").append(String.format("%.4f", r)).toString());
            }
            for (String set : quotaSet) {
                if (0 != quotaPairs.length()) {
                    quotaPairs.append(",");
                }
                quotaPairs.append(set);
            }
            sp = sp / n;
            sr = sr / n;
            sf = sf / n;
            newKey = batchId + "_01";
            newValue.append(quotaPairs)
                    .append("-")
                    .append(String.format("%.5f", sp))
                    .append(",")
                    .append(String.format("%.5f", sr))
                    .append(",")
                    .append(String.format("%.5f", sf));
            context.write(new Text(newKey), new Text(newValue.toString()));

            newKey = batchId + "_02";
            for (int i = 0; i < fList.size(); i++) {
                String f = fList.get(i);
                int count = 1;
                if (fMap.containsKey(f)) {
                    count += fMap.get(f);
                }
                fMap.put(f, count);
            }
            newValue = new StringBuilder("");
            for (Map.Entry<String, Integer> f : fMap.entrySet()) {
                if (0 != newValue.length()) {
                    newValue.append(",");
                }
                newValue.append(f.getKey()).append(":").append(String.valueOf(f.getValue()));
            }
            context.write(new Text(newKey), new Text(newValue.toString()));
        }
    }

}