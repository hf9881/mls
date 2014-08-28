package org.hf.mls.pref.hadoop.entropy;

import org.hf.mls.pref.hadoop.sum.SumCombiner;
import org.hf.mls.pref.hadoop.sum.SumReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by He Fan on 2014/7/16.
 * <p/>
 * Input format: <User,Item,P1,P2...></>
 * Output format:<Sum(P1)   Sum(P2)...></>
 * <p/>
 * The children job will get Sum(i) one by one
 */
public class QuotaCalculateJob extends Configured implements Tool {
    /**
     * @param args arg0: Input path
     *             arg1: Output path
     *             arg2: Batch id
     *             arg3: Prefs count
     * @return
     * @throws Exception
     */
    public static int getSum(String[] args) throws Exception {
        return ToolRunner.run(new Configuration(), new QuotaCalculateJob(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("pc", Integer.parseInt(args[3]));

        Job job = new Job(conf, "PREF_Quota Calculate_" + args[2]);
        job.setJarByClass(QuotaCalculateJob.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(SumMapper.class);
        job.setReducerClass(SumReducer.class);
        job.setCombinerClass(SumCombiner.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        boolean success = job.waitForCompletion(true);
        if (success) {
            return (int) job.getCounters().findCounter("Counter", "line").getValue();
        } else {
            System.out.println("Quota job failed!");
            return -1;
        }
    }

    public static class SumMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int prefCount = conf.getInt("pc", 1);

            Text newKey = new Text("1");
            String[] items = value.toString().replace("\t", "").split(",");
            StringBuilder newValue = new StringBuilder("");
            for (int i = 0; i < prefCount; i++) {
                if (0 != newValue.length()) {
                    newValue.append(",");
                }
                newValue.append(items[i + 2]);
            }
            context.getCounter("Counter", "line").increment(1);
            context.write(newKey, new Text(newValue.toString()));
        }
    }

}
