package org.hf.mls.pref.hadoop;

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
import java.util.ArrayList;

/**
 * Created by He Fan on 2014/5/29.
 */
public class PreProcessJob extends Configured implements Tool {

    public static long runJob(String[] args) throws Exception {
        return ToolRunner.run(new Configuration(), new PreProcessJob(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("BatchId", args[2]);
        conf.setInt("userIdPosition", Integer.parseInt(args[3]));
        conf.setInt("itemIdPosition", Integer.parseInt(args[4]));
        conf.set("splitChar", args[5]);
        conf.setInt("prefCount", Integer.parseInt(args[6]));
        conf.set("prefsPositions", args[7]);

        Job job = new Job(conf, "PREF_Data Pre_" + args[2]);
        job.setJarByClass(PreProcessJob.class);

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

            String newKey = "";
            String newValue = "";

            Configuration mapConf = context.getConfiguration();

            String splitChar = mapConf.get("splitChar", "-1");
            String batchId = mapConf.get("BatchId", "");

            int userIdPosition = mapConf.getInt("userIdPosition", -1);
            int itemIdPosition = mapConf.getInt("itemIdPosition", -1);
            int prefCount = mapConf.getInt("prefCount", 0);
            String prefValuePositions = mapConf.get("prefsPositions", "");

            if (-1 == userIdPosition || -1 == itemIdPosition || "-1".equals(splitChar)) {
                System.out.println("[ERROR]UserId/ItemId position or split char lost!");
                return;
            }

            String[] prefIndexes = prefValuePositions.split("-");

            String[] fields = value.toString().split(splitChar);
            if (fields.length > prefCount + 1) {
                String userId = fields[userIdPosition];
                String itemId = fields[itemIdPosition];
                newKey = batchId + "_" + userId + "," + itemId;

                if (prefCount == prefIndexes.length) {
                    for (int i = 0; i < prefCount; i++) {
                        if ("".equals(newValue)) {
                            newValue = fields[Integer.parseInt(prefIndexes[i])];
                        } else {
                            newValue += "," + fields[Integer.parseInt(prefIndexes[i])];
                        }
                    }
                    context.write(new Text(newKey), new Text(newValue));
                } else {
                    throw new IOException("[error]" + prefCount + ":" + prefValuePositions);
                }
            }
        }
    }

    public static class PreReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Configuration mapConf = context.getConfiguration();
            int prefCount = mapConf.getInt("prefCount", 0);

            if (0 != prefCount) {
                StringBuilder newKey = new StringBuilder(key.toString());
                int n = 0;
                ArrayList<String> pValues = new ArrayList<String>();

                for (Text value : values) {
                    pValues.add(value.toString());
                    n++;
                }
                if (1 == n) {
                    newKey.append(",").append(pValues.get(0));
                } else {
                    int[] finalValues = new int[prefCount];
                    for (int i = 0; i < prefCount; i++) {
                        finalValues[i] = 0;
                    }
                    for (String value : pValues) {
                        String[] prefValues = value.toString().split(",");
                        for (int i = 0; i < prefCount; i++) {
                            if (0 == finalValues[i]) {
                                finalValues[i] = Integer.parseInt(prefValues[i]);
                            } else {
                                finalValues[i] += Integer.parseInt(prefValues[i]);
                            }
                        }
                    }
                    for (int i = 0; i < prefCount; i++) {
                        newKey.append(",").append(String.valueOf(((double) finalValues[i])));
                    }
                }
                context.write(new Text(newKey.toString()), new Text(""));
            }

        }
    }
}