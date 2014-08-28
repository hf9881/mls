package org.hf.mls.ar.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by He Fan on 2014/4/9.
 */
public class EvaluateJob extends Configured implements Tool {

    public static long getCount(String[] args) throws Exception {

        return ToolRunner.run(new Configuration(), new EvaluateJob(), args);
    }

    @Override
    public int run(String[] args) throws Exception {


        Configuration conf = new Configuration();

        conf.setStrings("BatchId", args[2]);
        conf.setLong("N", Long.parseLong(args[3]));

        Job job = new Job(conf, "AR_Evaluate_" + args[2]);
        job.setJarByClass(EvaluateJob.class);

        FileInputFormat.setInputPaths(job, new Path(args[0] + "/fpgrowth"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapperClass(CalcSupportMapper.class);
        ChainReducer.setReducer(job, CalcSupportReducer.class, Text.class, Text.class, Text.class, Text.class, null);
        ChainReducer.addMapper(job, EvaluateMapper.class, Text.class, Text.class, Text.class, Text.class, null);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        boolean success = job.waitForCompletion(true);
        if (success) {
            return 1;
        } else {
            System.out.println("Eval Job failed!");
            return 0;
        }
    }

    public static class CalcSupportMapper extends Mapper<Writable, Writable, Text, Text> {

        public static final String str_split = "\t";

        @Override
        public void map(Writable key, Writable value, Context context) throws IOException, InterruptedException {

            String keyField = value.toString();
            String k = "";
            StringBuilder v = new StringBuilder("");
            String item_id1 = "";
            String item_id2 = "";

            if (keyField.length() > 1) {
                String[] str_mid0 = keyField.split("\\)");
                for (String str_rec0 : str_mid0) {
                    if (str_rec0.split(",").length > 1) {
                        str_rec0 = str_rec0.split("\\(")[1];
                        v.append(str_rec0).append(str_split);
                    }
                }

                String[] fields = v.toString().split(str_split);
                int len_fields = fields.length;
                if (len_fields > 0) {
                    String countX = "";
                    if (2 == fields[0].split(",").length) {
                        countX = fields[0].split(",")[1];
                    } else {
                        countX = fields[0].split(",")[fields[0].split(",").length - 1];
                    }

                    for (String str_mid : fields) {
                        String[] str_item = str_mid.split(",");
                        int len_item = str_item.length;
                        if (len_item == 3) {
                            item_id1 = str_item[0].split("\\[")[1];
                            item_id2 = str_item[1].split("\\]")[0].split(" ")[1];
                            k = item_id1;
                            v = new StringBuilder(item_id2);
                            v.append(",").append(str_item[2]).append(",").append(countX);
                            context.write(new Text(k), new Text(v.toString()));
                        } else if (len_item == 2) {
                            item_id1 = str_item[0].split("\\[")[1].split("]")[0];
                            k = item_id1;
                            v = new StringBuilder(countX);
                            context.write(new Text(k), new Text(v.toString()));
                        }
                    }
                }
            }


        }
    }

    public static class CalcSupportReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuilder k = new StringBuilder(key.toString());
            StringBuilder v = new StringBuilder("");
            String[] str_values = new String[1001];
            int n = 1;

            for (Text tmp_value : values) {
                String str_value = tmp_value.toString();
                String[] items = str_value.split(",");
                if (items.length == 3) {
                    if (n >= str_value.length() - 1) {
                        str_values = Arrays.copyOf(str_values, n + 100);
                    }
                    str_values[n] = str_value;
                    n++;
                } else {
                    str_values[0] = str_value;
                }
            }

            for (int i = 1; i < n; i++) {
                String[] str_items = str_values[i].split(",");
                k.append(",").append(str_items[0]);
                v.append(str_items[1]).append(",").append(str_items[2]).append(",").append(str_values[0]);
                context.write(new Text(k.toString()), new Text(v.toString()));
            }
        }
    }

    public static class EvaluateMapper extends Mapper<Text, Text, Text, Text> {

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

            Configuration map_conf = context.getConfiguration();
            long N = map_conf.getLong("N", 0);
            String batchId = map_conf.getStrings("BatchId", "")[0];

            String k = key.toString().replace("\t", " ").trim();
            String[] fields = value.toString().split(",");
            StringBuilder newKey = new StringBuilder(batchId);

            //只处理有效记录
            if (fields.length > 2) {
                //计算提升度
                double lift = (Double.parseDouble(fields[0]) * N) / (Double.parseDouble(fields[1]) * Double.parseDouble(fields[2]));
                String v = String.valueOf(lift);
                v = v.replace("\t", " ").trim();
                context.write(new Text(newKey.append("_").append(k.replace(",", "-")).append(",").append(v).toString()), new Text(""));
            }
        }
    }
}
