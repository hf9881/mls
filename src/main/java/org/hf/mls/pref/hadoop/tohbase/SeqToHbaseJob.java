package org.hf.mls.pref.hadoop.tohbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by He Fan on 2014/6/3.
 */
public class SeqToHbaseJob extends Configured implements Tool {

    public static long runJob(String[] args) throws Exception {
        return ToolRunner.run(new Configuration(), new SeqToHbaseJob(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setStrings("column_name", args[2]);

        Job job = new Job(conf, "PREF_Seq ToHbase_" + args[3]);
        job.setJarByClass(SeqToHbaseJob.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        TableMapReduceUtil.initTableReducerJob(args[1], null, job);

        job.setMapperClass(SeqHbaseMapper.class);

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

    public static class SeqHbaseMapper extends Mapper<Writable, Writable, ImmutableBytesWritable, Put> {

        @Override
        public void map(Writable key, Writable value, Context context) throws IOException, InterruptedException {
            Configuration map_conf = context.getConfiguration();
            String columnName = map_conf.getStrings("column_name", "")[0];

            String newKey = key.toString().replace("\t", " ").trim();
            String newValue = value.toString().replace("\t", " ").trim();

            byte[] rowKey = Bytes.toBytes(newKey);
            Put p = new Put(rowKey);
            p.add(Bytes.toBytes(columnName), Bytes.toBytes(""), Bytes.toBytes(newValue));
            context.write(new ImmutableBytesWritable(rowKey), p);
        }
    }
}