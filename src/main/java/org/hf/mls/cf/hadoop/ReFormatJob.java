package org.hf.mls.cf.hadoop;

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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.hf.mls.common.JobOptions;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;

/**
 * Created by He Fan on 2014/5/26.
 */
public class ReFormatJob {

    public static Job init(String[] args) throws IOException {
        System.out.println("ReFormat job started!");
        Configuration conf = new Configuration();
        conf.setDouble("percent", Double.parseDouble(args[3]));
        conf.setBoolean("booleanData", ("true".equals(args[5])));
        conf.set("TrainSetDir", args[6]);
        conf.set("TestSetDir", args[7]);

        Job job = new Job(conf, "CF_Reformat_" + args[4]);
        job.setJarByClass(ReFormatJob.class);

        //job.setInputFormatClass(SequenceFileInputFormat.class);
        //job.setOutputFormatClass(SequenceFileOutputFormat.class);

        if (JobOptions.COMPRESS.equals(args[2])) {
            FileOutputFormat.setCompressOutput(job, true);
            FileOutputFormat.setOutputCompressorClass(job, JobOptions.COMPRESS_CLASS);
            SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        }
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        MultipleOutputs.addNamedOutput(job, "MOSSeq", SequenceFileOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "MOSText", TextOutputFormat.class, Text.class, Text.class);

        job.setMapperClass(FormatMapper.class);
        job.setReducerClass(FormatReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job;
    }

    public static class FormatMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration mapConf = context.getConfiguration();
            boolean booleanData = mapConf.getBoolean("booleanData", true);

            String[] v = value.toString().split(",");
            String newKey = v[0];
            String newValue = v[1];
            if (!booleanData) {
                newValue += "\t" + v[2];
            }
            context.write(new Text(newKey), new Text(newValue));
        }
    }

    public static class FormatReducer extends Reducer<Text, Text, Text, Text> {
        private MultipleOutputs<Text, Text> mos;

        public void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs(context);
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Configuration mapConf = context.getConfiguration();
            boolean booleanData = mapConf.getBoolean("booleanData", true);
            String trainSetPath = mapConf.get("TrainSetDir", "");
            String testSetPath = mapConf.get("TestSetDir", "");

            double percent = context.getConfiguration().getDouble("percent", 0.7);
            StringBuilder newValue = new StringBuilder("");

            for (Text value : values) {
                if (0 != newValue.length()) {
                    newValue.append(",");
                }
                newValue.append(value.toString().trim());
            }
            //context.write(key, new Text(newValue.toString()));

            int itemLength = 0;
            int trainSetLength = 0;
            int testSetLength = 0;
            long Nr = 0;
            String[] items;
            //StringBuilder trainSet = new StringBuilder(key.toString());
            StringBuilder testSet = new StringBuilder("");

            items = newValue.toString().split(",");
            itemLength = items.length;
            if (itemLength > 0) {
                if (1 == itemLength) {
                    //trainSet.append(",").append(items[0]);
                    mos.write("MOSText", key, new Text(items[0]), trainSetPath);
                } else {
                    trainSetLength = (int) (itemLength * percent);
                    testSetLength = itemLength - trainSetLength;

                    int[] randTestSetIndex = getRandomSet(testSetLength, itemLength);

                    boolean isTrain = true;
                    for (int i = 0; i < itemLength; i++) {
                        for (int j = 0; j < testSetLength; j++) {
                            if (i == randTestSetIndex[j]) {
                                Nr++;
                                testSet.append(",");
                                if (booleanData) {
                                    testSet.append(items[i]);
                                } else {
                                    testSet.append(items[i].split("\t")[0]);
                                }
                                isTrain = false;
                                break;
                            }
                        }
                        if (isTrain) {
                            //trainSet.append(",").append(items[i]);
                            mos.write("MOSText", key, new Text(items[i].toString()), trainSetPath);
                        } else {
                            isTrain = true;
                        }
                    }
                    testSet = new StringBuilder("Nr").append(String.valueOf(Nr)).append(testSet);
                    mos.write("MOSSeq", key, new Text(testSet.toString()), testSetPath);
                    //context.write(new Text("2@b"), new Text(testSet.toString()));
                }
                //context.write(new Text("1@a"), new Text(trainSet.toString()));

            }
        }

        private int[] getRandomSet(int length, int max) {
            Random random = new Random();
            int[] rands = new int[length];
            HashSet<Integer> hashSet = new HashSet<Integer>();

            // 生成随机数字并存入HashSet
            while (hashSet.size() < length) {
                hashSet.add(random.nextInt(max));
            }
            for (int i = 0; i < length; i++) {
                rands[i] = Integer.parseInt(hashSet.toArray()[i].toString());
            }
            return rands;
        }
    }
}
