package org.hf.mls.ar.hadoop.tohbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by He Fan on 2014/5/29.
 */
public class ToHbaseJob extends Configured implements Tool {

    public static long extractData(String[] args) throws Exception {
        return ToolRunner.run(new Configuration(), new ToHbaseJob(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        conf.setStrings("columnName", args[2]);
        conf.setInt("splitCharNum", Integer.parseInt(args[3]));

        Job job = new Job(conf, "AR_ToHbase_" + args[2] + "_" + args[4]);
        job.setJarByClass(ToHbaseJob.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        TableMapReduceUtil.initTableReducerJob(args[1], null, job);

        job.setMapperClass(HbaseMapper.class);

        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        boolean success = job.waitForCompletion(true);
        if (success) {
            return 1;
        } else {
            return 0;
        }
    }

    public static class HbaseMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration mapConf = context.getConfiguration();

            String columnName = mapConf.getStrings("columnName", "")[0];
            String splitChar = "";
            if (1 == mapConf.getInt("splitCharNum", 0)) {
                splitChar = ",";
            } else if (2 == mapConf.getInt("splitCharNum", 0)) {
                splitChar = "\t";
            }

            if ("".equals(splitChar)) {
                throw new IOException("[ERROR]split char missed!");
            }
            if ("".equals(columnName)) {
                throw new IOException("[ERROR]column name missed!");
            }
            String newKey = value.toString().split(splitChar)[0].replace("\t", " ").trim();
            String newValue = value.toString().split(splitChar)[1].replace("\t", " ").trim();

            byte[] rowKey = Bytes.toBytes(newKey);
            Put p = new Put(rowKey);
            p.add(Bytes.toBytes(columnName), Bytes.toBytes(""), Bytes.toBytes(newValue));
            context.write(new ImmutableBytesWritable(rowKey), p);
        }
    }
}
