package org.hf.mls.ar.hadoop;

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
 * Created by He Fan on 2014/4/9.
 */
public class ExtractJob extends Configured implements Tool {

    public static long extractData(String[] args) throws Exception {
        return ToolRunner.run(new Configuration(), new ExtractJob(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setStrings("BatchId", args[2]);

        Job job = new Job(conf, "AR_Extract_" + args[2]);
        job.setJarByClass(ExtractJob.class);

        FileInputFormat.setInputPaths(job, new Path(args[0] + "/fpgrowth"));
        TableMapReduceUtil.initTableReducerJob(args[1], null, job);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        //job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapperClass(ExtractMapper.class);

        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        //job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(Text.class);

        boolean success = job.waitForCompletion(true);
        if (success) {
            return 1;
        } else {
            return 0;
        }
    }

    public static class ExtractMapper extends Mapper<Writable, Writable, ImmutableBytesWritable, Put> {

        @Override
        public void map(Writable key, Writable value, Context context) throws IOException, InterruptedException {
            Configuration map_conf = context.getConfiguration();
            String batchId = map_conf.getStrings("BatchId", "")[0];

            StringBuilder newKey = new StringBuilder(batchId);
            newKey.append("_").append(key.toString());
            StringBuilder newValue = new StringBuilder("");

            String[] valueFields = value.toString().split("\\), \\(");
            String[] keyFields1;
            String[] keyFields2;
            String[] keyFields3;

            if (valueFields.length > 1) {
                for (String tmpValue1 : valueFields) {
                    keyFields1 = tmpValue1.split("\\],");
                    for (String tmpValue2 : keyFields1) {
                        keyFields2 = tmpValue2.split("\\[");
                        for (String tmpValue3 : keyFields2) {
                            keyFields3 = tmpValue3.split(",");
                            if (keyFields3.length == 2) {
                                if (0 != newValue.length()) {
                                    newValue.append(",");
                                }
                                newValue.append(keyFields3[0]);
                            }
                        }
                    }

                }
                //context.write(new Text(newKey), new Text(newValue));
                byte[] rowKey = Bytes.toBytes(newKey.toString());
                Put p = new Put(rowKey);
                p.add(Bytes.toBytes("item_list"), Bytes.toBytes(""), Bytes.toBytes(newValue.toString()));
                context.write(new ImmutableBytesWritable(rowKey), p);
            }
        }
    }

}
