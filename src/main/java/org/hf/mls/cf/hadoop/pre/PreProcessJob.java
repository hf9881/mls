package org.hf.mls.cf.hadoop.pre;

import org.hf.mls.common.JobOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by He Fan on 2014/5/20.
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
        boolean booleanData = ("true".equals(args[6]));
        conf.setBoolean("booleanData", booleanData);
        if (!booleanData) {
            conf.setInt("prefValuePosition", Integer.parseInt(args[7]));
        }

        Job job = new Job(conf, "CF_Data pre_" + args[5]);
        job.setJarByClass(PreProcessJob.class);

        if (JobOptions.COMPRESS.equals(args[7])) {
            FileOutputFormat.setCompressOutput(job, true);
            FileOutputFormat.setOutputCompressorClass(job, JobOptions.COMPRESS_CLASS);
            SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        }

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(PreMapper.class);
        job.setReducerClass(PreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        boolean success = job.waitForCompletion(true);
        if (success) {
            return 1;
        } else {
            return 0;
        }
    }

    public static class PreMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration mapConf = context.getConfiguration();
            String splitChar = mapConf.getStrings("splitChar", "-1")[0];
            int userIdPosition = mapConf.getInt("userIdPosition", -1);
            int itemIdPosition = mapConf.getInt("itemIdPosition", -1);
            boolean booleanData = mapConf.getBoolean("booleanData", true);

            if (-1 == userIdPosition || -1 == itemIdPosition || "-1".equals(splitChar)) {
                System.out.println("[ERROR]UserId/ItemId position or split char lost!");
                return;
            }

            String[] fields = value.toString().split(splitChar);
            if (fields.length > 1) {
                StringBuilder userIdAndItemId = new StringBuilder(fields[userIdPosition])
                        .append(",")
                        .append(fields[itemIdPosition]);

                if (!booleanData) {
                    int prefValuePosition = mapConf.getInt("prefValuePosition", -1);
                    userIdAndItemId
                            .append(",")
                            .append(fields[prefValuePosition]);
                }
                context.write(new Text(userIdAndItemId.toString()), new Text(""));
            }
        }
    }

    public static class PreReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //排重
            context.write(key, new Text(""));
        }
    }
}
