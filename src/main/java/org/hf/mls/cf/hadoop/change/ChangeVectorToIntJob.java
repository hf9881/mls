package org.hf.mls.cf.hadoop.change;

import org.hf.mls.common.JobOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by He Fan on 2014/7/8.
 */
public class ChangeVectorToIntJob {
    public static Job init(String[] args) throws IOException {
        System.out.println("Change vector to int job started!");
        Configuration conf = new Configuration();
        conf.setInt("index", Integer.parseInt(args[4]));

        Job job = new Job(conf, "CF_Change Vector_" + args[3]);
        job.setJarByClass(ChangeVectorToIntJob.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]), new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        if (JobOptions.COMPRESS.equals(args[5])) {
            FileOutputFormat.setCompressOutput(job, true);
            FileOutputFormat.setOutputCompressorClass(job, JobOptions.COMPRESS_CLASS);
        }

        job.setMapperClass(ChangeMapper.class);
        job.setReducerClass(ChangeReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    public static class ChangeReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int index = conf.getInt("index", 0);

            String Id = "";
            ArrayList<String> items = new ArrayList();
            for (Text v : values) {
                String value = v.toString().replace("\t", "").trim();
                if (value.startsWith("_")) {
                    Id = value.substring(1);
                } else {
                    items.add(value);
                }
            }

            for (String item : items) {
                StringBuilder newKey = new StringBuilder("");
                if (0 == index) {
                    newKey.append(Id).append(",").append(item);
                } else {
                    String[] subItems = item.split(",");
                    for (int i = 0; i < subItems.length; i++) {
                        if (0 != i) {
                            newKey.append(",");
                        }
                        newKey.append(subItems[i]);
                        //append the dic index id
                        if (i == (index - 1)) {
                            newKey.append(",").append(Id);
                        }
                    }
                }
                context.write(new Text(newKey.toString()), new Text(""));
            }
        }
    }
}
