package org.hf.mls.cf.hadoop.change;

import org.hf.mls.common.JobOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by He Fan on 2014/7/8.
 */
public class ReduceNanJob {

    public static Job init(String[] args) throws IOException {
        String tag = "";
        if ("0".equals(args[4])) {
            tag = "User";
        } else {
            tag = "Item";
        }
        System.out.println("Reduction " + tag + " job started!");
        Configuration conf = new Configuration();
        conf.setInt("index", Integer.parseInt(args[4]));

        Job job = new Job(conf, "CF_Reduction " + tag + "_" + args[3]);
        job.setJarByClass(ReduceNanJob.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]), new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        if (JobOptions.COMPRESS.equals(args[5])) {
            FileOutputFormat.setCompressOutput(job, true);
            FileOutputFormat.setOutputCompressorClass(job, JobOptions.COMPRESS_CLASS);
            SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        }

        job.setMapperClass(ChangeMapper.class);
        job.setReducerClass(ItemReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    public static class ItemReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int itemIndex = conf.getInt("index", 0);

            StringBuilder newKey = new StringBuilder("");
            StringBuilder newValue = new StringBuilder("");
            ArrayList<String> items = new ArrayList<String>();
            String word = "";

            for (Text v : values) {
                String item = v.toString().replace("\t", "");
                if (item.startsWith("_")) {
                    word = item.substring(1);
                } else {
                    items.add(item);
                }
            }

            if (0 == itemIndex) {
                newKey.append(word);
                for (int i = 0; i < items.size(); i++) {
                    if (0 != newValue.length()) {
                        newValue.append(",");
                    }
                    newValue.append(items.get(i));
                }
                context.write(new Text(newKey.toString()), new Text(newValue.toString()));
            } else {
                for (int i = 0; i < items.size(); i++) {
                    newKey = new StringBuilder(items.get(i));
                    newKey.append(",").append(word);
                    context.write(new Text(newKey.toString()), new Text(""));
                }
            }

        }
    }
}
