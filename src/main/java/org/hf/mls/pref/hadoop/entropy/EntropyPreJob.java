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
 * Created by He Fan on 2014/7/17.
 * <p/>
 * Input format: <User,Item,P1,P2...></>
 * Output format:<H(P1)   H(P2)...></>
 * <p/>
 */
public class EntropyPreJob extends Configured implements Tool {
    /**
     * @param args arg0: Input path
     *             arg1: Output path
     *             arg2: Batch id
     *             arg3: Prefs count
     *             arg4: Pref Sums (v1,v2,v3...)
     * @return
     * @throws Exception
     */
    public static long runJob(String[] args) throws Exception {
        return ToolRunner.run(new Configuration(), new EntropyPreJob(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("pc", Integer.parseInt(args[3]));
        conf.set("prefSums", args[4]);

        Job job = new Job(conf, "PREF_Entropy_" + args[2]);
        job.setJarByClass(EntropyPreJob.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(EntropyMapper.class);
        job.setReducerClass(SumReducer.class);
        job.setCombinerClass(SumCombiner.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        boolean success = job.waitForCompletion(true);
        if (success) {
            return 1;
        } else {
            return 0;
        }
    }

    /**
     * output: 1  V1,V2...
     */
    public static class EntropyMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String pref = conf.get("prefSums");
            int prefCount = conf.getInt("pc", 1);
            double[] prefSum = new double[prefCount];

            String[] prefs = pref.split(",");

            for (int i = 0; i < prefCount; i++) {
                prefSum[i] = Double.parseDouble(prefs[i]);
            }

            String[] items = value.toString().replace("\t", "").split(",");
            StringBuilder newValue = new StringBuilder("");
            double f = 0;

            for (int i = 0; i < prefCount; i++) {
                if (0 != newValue.length()) {
                    newValue.append(",");
                }
                f = Double.parseDouble(items[i + 2]) / prefSum[i];
                f = f * Math.log(f);
                newValue.append(String.valueOf(f));
            }
            context.write(new Text("1"), new Text(newValue.toString()));
        }
    }
}
