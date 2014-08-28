package org.hf.mls.ar.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by He Fan on 2014/4/9.
 */
public class PreProcessJob extends Configured implements Tool {

    public static long runJob(String[] args) throws Exception {
        return ToolRunner.run(new Configuration(), new PreProcessJob(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("userIdPosition", Integer.parseInt(args[2]));
        conf.setInt("itemIdPosition", Integer.parseInt(args[3]));
        conf.setStrings("splitChar", args[4]);

        Job job = new Job(conf, "AR_Data pre_" + args[5]);
        job.setJarByClass(PreProcessJob.class);

        //job.setInputFormatClass(SequenceFileInputFormat.class);
        //job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(DataMapper.class);
        job.setReducerClass(DataReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        boolean success = job.waitForCompletion(true);
        if (success) {
            return 1;
        } else {
            return 0;
        }
    }

    public static class DataMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            Configuration mapConf = context.getConfiguration();
            String splitChar = mapConf.getStrings("splitChar", "-1")[0];
            int userIdPosition = mapConf.getInt("userIdPosition", -1);
            int itemIdPosition = mapConf.getInt("itemIdPosition", -1);

            if (-1 == userIdPosition || -1 == itemIdPosition || "-1".equals(splitChar)) {
                System.out.println("[ERROR]UserId/ItemId position or split char lost!");
                return;
            }

            String[] fields = value.toString().split(splitChar);
            if (fields.length > 1) {
                String userId = fields[userIdPosition];
                String itemId = fields[itemIdPosition];

                context.write(new Text(userId), new Text(itemId));
            }
        }
    }

    public static class DataReducer extends Reducer<Text, Text, Text, Text> {
        //fpg default split char is '\t'
        public static final String str_split = "\t";

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder newKey = new StringBuilder("");

            //将同一个user下的item放入一个队列中
            for (Text value : values) {
                if (!"".equals(value.toString())) {
                    if (0 != newKey.length()) {
                        newKey.append(str_split);
                    }
                    newKey.append(value.toString());
                }
            }
            context.write(new Text(newKey.toString()), new Text(""));
        }
    }
}
