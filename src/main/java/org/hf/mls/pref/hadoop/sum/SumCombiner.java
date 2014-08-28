package org.hf.mls.pref.hadoop.sum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by He Fan on 2014/7/21.
 *
 * input:   1  V1,V2...
 * output:  1  Ss1,Ss2...
 */
public class SumCombiner extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int prefCount = conf.getInt("pc", 1);
        StringBuilder newKey = new StringBuilder("");
        double[] prefValue = new double[prefCount];

        for (int i = 0; i < prefCount; i++) {
            prefValue[i] = 0;
        }
        for (Text v : values) {
            String[] items = v.toString().replace("\t", "").split(",");
            for (int i = 0; i < prefCount; i++) {
                prefValue[i] += Double.parseDouble(items[i]);
            }
        }

        for (int i = 0; i < prefCount; i++) {
            if (0 != newKey.length()) {
                newKey.append(",");
            }
            newKey.append(prefValue[i]);
        }
        context.write(new Text("1"), new Text(newKey.toString()));
    }
}