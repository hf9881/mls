package org.hf.mls.pref.hadoop.zscore;

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
 * Output format:<s(P1)   s(P2)...></>
 * <p/>
 */
public class ZscoreJob extends Configured implements Tool {
    /**
     * @param args arg0: Input path
     *             arg1: Output path
     *             arg2: Batch id
     *             arg3: Prefs count
     *             arg4: Pref Average (v1,v2,v3...)
     *             arg5: Pref Stander Deviation (s1,s2,s3...)
     *             arg6: Weight (w1,w2,w3...)
     * @return
     * @throws Exception
     */
    public static long runJob(String[] args) throws Exception {
        return ToolRunner.run(new Configuration(), new ZscoreJob(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("pc", Integer.parseInt(args[3]));
        conf.set("prefAver", args[4]);
        conf.set("prefStdDev", args[5]);
        conf.set("prefWeight", args[6]);

        Job job = new Job(conf, "PREF_Zscore_" + args[2]);
        job.setJarByClass(ZscoreJob.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(ZscoreMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        boolean success = job.waitForCompletion(true);
        if (success) {
            return 1;
        } else {
            return 0;
        }
    }

    public static class ZscoreMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //获取参数
            Configuration conf = context.getConfiguration();
            String[] prefAver = conf.get("prefAver").split(",");
            String[] prefStdDev = conf.get("prefStdDev").split(",");
            String[] prefWeight = conf.get("prefWeight").split(",");

            int prefCount = conf.getInt("pc", 1);
            double factor = 0;
            double[] prefAvers = new double[prefCount];
            double[] prefStdDevs = new double[prefCount];
            double[] prefWeights = new double[prefCount];


            for (int i = 0; i < prefCount; i++) {
                prefAvers[i] = Double.parseDouble(prefAver[i]);
                prefStdDevs[i] = Double.parseDouble(prefStdDev[i]);
                prefWeights[i] = Double.parseDouble(prefWeight[i]);
            }

            String[] items = value.toString().replace("\t", "").split(",");
            StringBuilder newKey = new StringBuilder(items[0]);
            newKey.append("\t").append(items[1]).append("\t");
            double newPrefValue = 0;

            for (int i = 0; i < prefCount; i++) {
                factor = (prefAvers[i] - Double.parseDouble(items[i + 2])) / prefStdDevs[i];
                newPrefValue += (prefWeights[i] / (1 + Math.exp(factor)));
            }
            newPrefValue *= 5;
            newKey.append(String.valueOf(newPrefValue));
            context.write(new Text(newKey.toString()), new Text(""));
        }
    }
}
