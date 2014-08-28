package org.hf.mls.cf.hadoop.change;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by He Fan on 2014/7/9.
 */
public class ChangeMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int itemIndex = conf.getInt("index", 0);

        String[] items = value.toString().replace("\t", "").trim().split(",");
        String newKey = "";
        StringBuilder newValue = new StringBuilder("");

        for (int i = 0; i < items.length; i++) {
            if (i == itemIndex) {
                newKey = items[itemIndex];
            } else {
                if (!"".equals(items[i]) && null != items[i]) {
                    if (newValue.length() > 0) {
                        newValue.append(",");
                    }
                    newValue.append(items[i]);
                }
            }
        }

        Text word = new Text(newKey);
        Text item = new Text(newValue.toString());
        context.write(word, item);
    }
}