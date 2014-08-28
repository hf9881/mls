package org.hf.mls.cf.hadoop.tohbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import java.io.IOException;

/**
 * Created by He Fan on 2014/5/30.
 */
public class SeqToHbaseJob {

    public static Job init(String[] args) throws IOException {
        System.out.println("Static data To Hbase Job started!");
        Configuration conf = new Configuration();
        conf.setStrings("column_name", args[3]);

        Job job = new Job(conf, "CF_SequenceToHbase_" + args[2]);
        job.setJarByClass(SeqToHbaseJob.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        TableMapReduceUtil.initTableReducerJob(args[1], null, job);

        job.setMapperClass(SeqHbaseMapper.class);

        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        return job;
    }

    public static class SeqHbaseMapper extends Mapper<Writable, Writable, ImmutableBytesWritable, Put> {

        @Override
        public void map(Writable key, Writable value, Context context) throws IOException, InterruptedException {
            Configuration map_conf = context.getConfiguration();
            String[] columnName = map_conf.getStrings("column_name", "")[0].split(":");
            String family = columnName[0];
            String qualifier0 = columnName[1];
            String qualifier1 = columnName[2];

            String newKey = key.toString().replace("\t", " ").trim();
            String[] newValue = value.toString().replace("\t", " ").trim().split("-");

            String statisticList = "";
            String quotaMean = "";
            byte[] rowKey = Bytes.toBytes(newKey);
            Put p = new Put(rowKey);

            if (newValue.length > 1) {
                quotaMean = newValue[1];
                p.add(Bytes.toBytes(family), Bytes.toBytes(qualifier1), Bytes.toBytes(quotaMean));
            }
            statisticList = newValue[0];
            p.add(Bytes.toBytes(family), Bytes.toBytes(qualifier0), Bytes.toBytes(statisticList));

            context.write(new ImmutableBytesWritable(rowKey), p);
        }
    }

}