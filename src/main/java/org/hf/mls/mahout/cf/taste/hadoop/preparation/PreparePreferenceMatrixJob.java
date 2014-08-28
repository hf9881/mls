package org.hf.mls.mahout.cf.taste.hadoop.preparation;

import org.hf.mls.mahout.cf.taste.hadoop.item.ToUserVectorsReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.cf.taste.hadoop.EntityPrefWritable;
import org.apache.mahout.cf.taste.hadoop.ToEntityPrefsMapper;
import org.apache.mahout.cf.taste.hadoop.ToItemPrefsMapper;
import org.apache.mahout.cf.taste.hadoop.item.ItemIDIndexMapper;
import org.apache.mahout.cf.taste.hadoop.item.ItemIDIndexReducer;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.VectorWritable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by He Fan on 2014/7/3.
 */
public class PreparePreferenceMatrixJob extends AbstractJob {

    public static final String NUM_USERS = "numUsers.bin";
    public static final String ITEMID_INDEX = "itemIDIndex";
    public static final String USER_VECTORS = "userVectors";
    public static final String RATING_MATRIX = "ratingMatrix";

    private static final int DEFAULT_MIN_PREFS_PER_USER = 1;

    public Map<String, ControlledJob> getJobs(String[] args) throws Exception {

        addInputOption();
        addOutputOption();
        addOption("maxPrefsPerUser", "mppu", "max number of preferences to consider per user, "
                + "users with more preferences will be sampled down");
        addOption("minPrefsPerUser", "mp", "ignore users with less preferences than this "
                + "(default: " + DEFAULT_MIN_PREFS_PER_USER + ')', String.valueOf(DEFAULT_MIN_PREFS_PER_USER));
        addOption("booleanData", "b", "Treat input as without pref values", Boolean.FALSE.toString());
        addOption("ratingShift", "rs", "shift ratings by this value", "0.0");

        Map<String, List<String>> parsedArgs = parseArguments(args);
        if (parsedArgs == null) {
            return null;
        }

        int minPrefsPerUser = Integer.parseInt(getOption("minPrefsPerUser"));
        boolean booleanData = Boolean.valueOf(getOption("booleanData"));
        float ratingShift = Float.parseFloat(getOption("ratingShift"));

        Map<String, ControlledJob> cJobs = new HashMap<String, ControlledJob>();

        ControlledJob cItemIDIndex = new ControlledJob(new Configuration());
        ControlledJob cToUserVectors = new ControlledJob(new Configuration());
        ControlledJob cToItemVectors = new ControlledJob(new Configuration());

        //convert items to an internal index
        Job itemIDIndex = prepareJob(getInputPath(),
                getOutputPath(ITEMID_INDEX),
                TextInputFormat.class,
                ItemIDIndexMapper.class, VarIntWritable.class, VarLongWritable.class,
                ItemIDIndexReducer.class,
                VarIntWritable.class, VarLongWritable.class,
                SequenceFileOutputFormat.class);
        itemIDIndex.setCombinerClass(ItemIDIndexReducer.class);
        /**
         * the job's depending will definite outside
         */

        cItemIDIndex.setJob(itemIDIndex);
        cJobs.put("itemIDIndex", cItemIDIndex);

        //convert user preferences into a vector per user
        Job toUserVectors = prepareJob(getInputPath(),
                getOutputPath(USER_VECTORS),
                TextInputFormat.class,
                ToItemPrefsMapper.class,
                VarLongWritable.class,
                booleanData ? VarLongWritable.class : EntityPrefWritable.class,
                ToUserVectorsReducer.class,
                VarLongWritable.class,
                VectorWritable.class,
                SequenceFileOutputFormat.class);
        toUserVectors.getConfiguration().setBoolean(RecommenderJob.BOOLEAN_DATA, booleanData);
        toUserVectors.getConfiguration().setInt(ToUserVectorsReducer.MIN_PREFERENCES_PER_USER, minPrefsPerUser);
        toUserVectors.getConfiguration().set(ToEntityPrefsMapper.RATING_SHIFT, String.valueOf(ratingShift));
        /**
         * write the number of users to hdfs
         * in method override cleanup()
         * write to path: getOutputPath(NUM_USERS)
         */
        toUserVectors.getConfiguration().set("NUM_USERS", getOutputPath(NUM_USERS).toString());
        /**
         * the job's depending will definite outside
         */

        cToUserVectors.setJob(toUserVectors);
        cJobs.put("toUserVectors", cToUserVectors);

        //we need the number of users later
        //build the rating matrix
        Job toItemVectors = prepareJob(getOutputPath(USER_VECTORS), getOutputPath(RATING_MATRIX),
                ToItemVectorsMapper.class, IntWritable.class, VectorWritable.class, ToItemVectorsReducer.class,
                IntWritable.class, VectorWritable.class);
        toItemVectors.setCombinerClass(ToItemVectorsReducer.class);

        /* configure sampling regarding the uservectors */
        if (hasOption("maxPrefsPerUser")) {
            int samplingSize = Integer.parseInt(getOption("maxPrefsPerUser"));
            toItemVectors.getConfiguration().setInt(ToItemVectorsMapper.SAMPLE_SIZE, samplingSize);
        }
        /**
         * this last job which is depended by next task's first job
         */

        cToItemVectors.setJob(toItemVectors);
        cToItemVectors.addDependingJob(cToUserVectors);
        cJobs.put("toItemVectors", cToItemVectors);

        return cJobs;
    }

    @Override
    public int run(String[] args) throws Exception {
        return 0;
    }
}
