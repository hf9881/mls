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
 * Created by He Fan on 2014/5/27.
 */
public class ExtractJob {

    public static Job init(String[] args) throws IOException {
        System.out.println("Extract cf to Hbase job started!");
        Configuration conf = new Configuration();
        conf.setStrings("BatchId", args[2]);

        Job job = new Job(conf, "CF_Extract_item_list_" + args[2]);
        job.setJarByClass(ExtractJob.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        TableMapReduceUtil.initTableReducerJob(args[1], null, job);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        job.setMapperClass(ExtractMapper.class);
        job.setNumReduceTasks(0);

        return job;
    }

    public static class ExtractMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration map_conf = context.getConfiguration();
            String batchId = map_conf.getStrings("BatchId", "")[0];

            StringBuilder newKey = new StringBuilder("");
            StringBuilder newValue = new StringBuilder("");
            int length = 0;

            String[] fields = value.toString().split("\t");
            String userId = fields[0];

            String mid_1 = fields[1].split("\\[")[1];
            String[] mid_2 = mid_1.split(",");
            length = mid_2.length;

            for (int i = 0; i < length; i++) {
                String itemId = mid_2[i].split(":")[0];
                if (0 != newValue.length()) {
                    newValue.append(",");
                }
                newValue.append(itemId);
            }

            newKey.append(batchId).append("_").append(userId);

            byte[] rowKey = Bytes.toBytes(newKey.toString());
            Put p = new Put(rowKey);
            p.add(Bytes.toBytes("item_list"), Bytes.toBytes(""), Bytes.toBytes(newValue.toString()));
            context.write(new ImmutableBytesWritable(rowKey), p);
        }
    }
}