package org.hf.mls.cf.hadoop.tohbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Created by He Fan on 2014/5/30.
 */
public class TextToHbaseJob {

    public static Job init(String[] args) throws IOException {
        System.out.println("EvalList To Hbase Job started!");
        Configuration conf = new Configuration();
        conf.set("column_name", args[3]);
        conf.set("batchid", args[2]);

        Job job = new Job(conf, "CF_TextToHbase_" + args[3] + "_" + args[2]);
        job.setJarByClass(TextToHbaseJob.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        TableMapReduceUtil.initTableReducerJob(args[1], null, job);

        job.setMapperClass(TextHbaseMapper.class);

        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        return job;
    }

    public static class TextHbaseMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String columnName = conf.get("column_name", "");
            String newKey = "";
            String newValue = "";

            if ("item_list".equals(columnName)) {
                String batchId = conf.get("batchid", "");
                String[] items = value.toString().split("\t");
                if (items.length > 1) {
                    newKey = batchId + "_" + items[0];
                    newValue = items[1];
                } else {
                    return;
                }
            } else {
                newKey = value.toString().split(",")[0].replace("\t", " ").trim();
                String precision = value.toString().split(",")[1].replace("\t", " ").trim();
                String recall = value.toString().split(",")[2].replace("\t", " ").trim();
                String f = value.toString().split(",")[3].replace("\t", " ").trim();
                newValue = precision + "," + recall + "," + f;
            }

            byte[] rowKey = Bytes.toBytes(newKey);
            Put p = new Put(rowKey);
            p.add(Bytes.toBytes(columnName), Bytes.toBytes(""), Bytes.toBytes(newValue));
            context.write(new ImmutableBytesWritable(rowKey), p);
        }
    }


}
