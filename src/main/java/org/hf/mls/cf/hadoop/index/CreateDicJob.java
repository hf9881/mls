package org.hf.mls.cf.hadoop.index;

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
 * Created by He Fan on 2014/7/7.
 */
public class CreateDicJob {
    public static Job init(String[] args) throws IOException {
        String tag = "";
        if ("0".equals(args[3])) {
            tag = "User";
        } else {
            tag = "Item";
        }
        System.out.println("Create dic " + tag + "job started!");
        Configuration conf = new Configuration();
        conf.setInt("index", Integer.parseInt(args[3]));
        //conf.setBoolean("ispref", args[4].equals("false"));

        Job job = new Job(conf, "CF_Create " + tag + " Dic_" + args[2]);
        job.setJarByClass(CreateDicJob.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (JobOptions.COMPRESS.equals(args[4])) {
            FileOutputFormat.setCompressOutput(job, true);
            FileOutputFormat.setOutputCompressorClass(job, JobOptions.COMPRESS_CLASS);
            SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        }

        job.setMapperClass(DicMapper.class);
        job.setReducerClass(DicReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //job.getCounters().findCounter("Counter", "line").setValue(0);
        return job;
    }

    public static class DicMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int index = conf.getInt("index", 2);
            //boolean isPref = conf.getBoolean("ispref", true);

            String[] items = value.toString().split(",");
            if (items.length > index) {
                String newKey = items[index];
                Text word = new Text(newKey);
                Text nullText = new Text("");
                context.write(word, nullText);
            }
        }
    }

    public static class DicReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int index = conf.getInt("index", 2);
            context.getCounter("Counter", "line").increment(1);
            long num = context.getCounter("Counter", "line").getValue();

            //if I create user index dic then the format is: User,_Index
            //if I create item index dic then the format is: ,Item,_Index
            StringBuilder newKey = new StringBuilder(key.toString());
            if (1 == index) {
                newKey.insert(0, ",");
            }
            newKey.append(",_").append(String.valueOf(num));
            Text word = new Text(newKey.toString());
            Text nullText = new Text("");
            context.write(word, nullText);
        }
    }
}
