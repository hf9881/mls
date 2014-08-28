package org.hf.mls.mahout.cf.taste.hadoop.item;

/**
 * Created by He Fan on 2014/6/18.
 */

import org.hf.mls.mahout.cf.taste.hadoop.preparation.PreparePreferenceMatrixJob;
import org.hf.mls.mahout.math.hadoop.similarity.cooccurrence.RowSimilarityJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.EntityEntityWritable;
import org.apache.mahout.cf.taste.hadoop.RecommendedItemsWritable;
import org.apache.mahout.cf.taste.hadoop.item.*;
import org.apache.mahout.cf.taste.hadoop.similarity.item.ItemSimilarityJob;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.VectorSimilarityMeasures;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class RecommenderJob extends AbstractJob {
    public static final String BOOLEAN_DATA = "booleanData";
    private static final int DEFAULT_MAX_SIMILARITIES_PER_ITEM = 100;
    private static final int DEFAULT_MAX_PREFS_PER_USER = 1000;
    private static final int DEFAULT_MIN_PREFS_PER_USER = 1;

    /**
     * @param args
     * @return job map list (with dependence)
     * itemIDIndex  (reFormatJob)
     * toUserVectors  (reFormatJob)
     * toItemVectors  (toUserVectors)
     * //countObservations  (toItemVectors)
     * normsAndTranspose  (toItemVectors)
     * pairwiseSimilarity  (normsAndTranspose)
     * asMatrix  (pairwiseSimilarity)
     * outputPathForSimilarityMatrix  (asMatrix)
     * partialMultiply  (asMatrix+toUserVectors)
     * itemFiltering  (none)
     * aggregateAndRecommend  (partialMultiply+itemFiltering)
     * @throws Exception
     */
    /**
     * job name									depending
     * -----------------------Controller1-------------------------------------------------
     * reFormatJob								preProcessJob
     * <p/>
     * itemIDIndex								reFormatJob
     * toUserVectors							reFormatJob
     * toItemVectors							toUserVectors
     * <p/>
     * //countObservations						toItemVectors
     * normsAndTranspose					    toItemVectors
     * pairwiseSimilarity						normsAndTranspose
     * asMatrix									pairwiseSimilarity
     * <p/>
     * outputPathForSimilarityMatrix	        asMatrix
     * partialMultiply							asMatrix+toUserVectors
     * itemFiltering							none
     * aggregateAndRecommend		            partialMultiply+(itemFiltering)
     * <p/>
     * extractEvalJob							aggregateAndRecommend
     * calculateNrsJob							extractEvalJob
     * evaluateJob								calculateNrsJob
     * textToHbaseJob							evaluateJob
     * preStaticJob								evaluateJob
     * seqToHbaseJob							preStaticJob
     * -----------------------over-------------------------------
     * <p/>
     * -------------------------Controller2-----------------------------------------------
     * preProcessJob		(do not put it into Controller)
     * <p/>
     * itemIDIndex								none
     * toUserVectors							none
     * toItemVectors							toUserVectors
     * <p/>
     * //countObservations						toItemVectors
     * normsAndTranspose					    toItemVectors
     * pairwiseSimilarity						normsAndTranspose
     * asMatrix									pairwiseSimilarity
     * <p/>
     * outputPathForSimilarityMatrix	        asMatrix
     * partialMultiply							asMatrix+toUserVectors
     * itemFiltering							none
     * aggregateAndRecommend		            partialMultiply+(itemFiltering)
     * <p/>
     * extractJob								aggregateAndRecommend
     * -----------------------over-------------------------------
     */
    public Map<String, ControlledJob> getJobs(String[] args) throws Exception {
        System.out.println("[INFO] Mahout cf job start!");
        addInputOption();
        addOutputOption();
        addOption("numRecommendations", "n", "Number of recommendations per user", String.valueOf(10));

        addOption("usersFile", null, "File of users to recommend for", null);
        addOption("itemsFile", null, "File of items to recommend for", null);
        addOption("filterFile", "f", "File containing comma-separated userID,itemID pairs. Used to exclude the item from the recommendations for that user (optional)", null);

        addOption("booleanData", "b", "Treat input as without pref values", Boolean.FALSE.toString());
        addOption("maxPrefsPerUser", "mxp", "Maximum number of preferences considered per user in final recommendation phase", String.valueOf(10));

        addOption("minPrefsPerUser", "mp", "ignore users with less preferences than this in the similarity computation (default: 1)", String.valueOf(1));

        addOption("maxSimilaritiesPerItem", "m", "Maximum number of similarities considered per item ", String.valueOf(100));

        addOption("maxPrefsPerUserInItemSimilarity", "mppuiis", "max number of preferences to consider per user in the item similarity computation phase, users with more preferences will be sampled down (default: 1000)", String.valueOf(1000));

        addOption("similarityClassname", "s", "Name of distributed similarity measures class to instantiate, alternatively use one of the predefined similarities (" + VectorSimilarityMeasures.list() + ')', true);

        addOption("threshold", "tr", "discard item pairs with a similarity value below this", false);
        addOption("outputPathForSimilarityMatrix", "opfsm", "write the item similarity matrix to this path (optional)", false);

        Map parsedArgs = parseArguments(args);
        if (parsedArgs == null) {
            return null;
        }

        Path outputPath = getOutputPath();
        int numRecommendations = Integer.parseInt(getOption("numRecommendations"));
        String usersFile = getOption("usersFile");
        String itemsFile = getOption("itemsFile");
        String filterFile = getOption("filterFile");
        boolean booleanData = Boolean.valueOf(getOption("booleanData")).booleanValue();
        int maxPrefsPerUser = Integer.parseInt(getOption("maxPrefsPerUser"));
        int minPrefsPerUser = Integer.parseInt(getOption("minPrefsPerUser"));
        int maxPrefsPerUserInItemSimilarity = Integer.parseInt(getOption("maxPrefsPerUserInItemSimilarity"));
        int maxSimilaritiesPerItem = Integer.parseInt(getOption("maxSimilaritiesPerItem"));
        String similarityClassname = getOption("similarityClassname");
        double threshold = hasOption("threshold") ? Double.parseDouble(getOption("threshold")) : 4.9E-324D;

        Path prepPath = getTempPath("preparePreferenceMatrix");
        Path similarityMatrixPath = getTempPath("similarityMatrix");
        Path explicitFilterPath = getTempPath("explicitFilterPath");
        Path partialMultiplyPath = getTempPath("partialMultiply");

        AtomicInteger currentPhase = new AtomicInteger();

        int numberOfUsers = 0;

        //Collection<ControlledJob> cJobs = new HashSet<ControlledJob>();
        Map<String, ControlledJob> mJobs = new HashMap<String, ControlledJob>();
        Map<String, ControlledJob> preparePreferenceMatrixJobs = new HashMap<String, ControlledJob>();
        Map<String, ControlledJob> rowSimilarityJobs = new HashMap<String, ControlledJob>();

        ControlledJob cOutputSimilarityMatrix = null;
        ControlledJob cPartialMultiply = null;
        ControlledJob cItemFiltering = null;
        ControlledJob cAggregateAndRecommend = null;

        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            /**
             *old code: ToolRunner.run(getConf(),
             *           new PreparePreferenceMatrixJob(),
             *           new String[]{"--input", getInputPath().toString(),
             *           "--output", prepPath.toString(), "--maxPrefsPerUser",
             *           String.valueOf(maxPrefsPerUserInItemSimilarity),
             *           "--minPrefsPerUser", String.valueOf(minPrefsPerUser),
             *           "--booleanData", String.valueOf(booleanData), "--tempDir", getTempPath().toString()});
             *           numberOfUsers = HadoopUtil.readInt(new Path(prepPath, "numUsers.bin"), getConf());
             */
            PreparePreferenceMatrixJob ppmcj = new PreparePreferenceMatrixJob();
            preparePreferenceMatrixJobs = ppmcj.getJobs(new String[]{"--input", getInputPath().toString(),
                    "--output", prepPath.toString(), "--maxPrefsPerUser", String.valueOf(maxPrefsPerUserInItemSimilarity),
                    "--minPrefsPerUser", String.valueOf(minPrefsPerUser), "--booleanData", String.valueOf(booleanData),
                    "--tempDir", getTempPath().toString()});
            mJobs.putAll(preparePreferenceMatrixJobs);
        }

        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            /**
             *old code:
             *if (numberOfUsers == -1) {
             * numberOfUsers = (int) HadoopUtil.countRecords(new Path(prepPath, "userVectors"), PathType.LIST, null, getConf());
             * }
             * ToolRunner.run(getConf(), new RowSimilarityJob(), new String[]{"--input", new Path(prepPath, "ratingMatrix").toString(), "--output", similarityMatrixPath.toString(), "--numberOfColumns", String.valueOf(numberOfUsers), "--similarityClassname", similarityClassname, "--maxSimilaritiesPerRow", String.valueOf(maxSimilaritiesPerItem), "--excludeSelfSimilarity", String.valueOf(Boolean.TRUE), "--threshold", String.valueOf(threshold), "--tempDir", getTempPath().toString()});
             */
            RowSimilarityJob rsj = new RowSimilarityJob();
            rowSimilarityJobs = rsj.getJobs(new String[]{"--input", prepPath.toString(),
                    "--output", similarityMatrixPath.toString(), "--numberOfColumns", String.valueOf(numberOfUsers),
                    "--similarityClassname", similarityClassname, "--maxSimilaritiesPerRow", String.valueOf(maxSimilaritiesPerItem),
                    "--excludeSelfSimilarity", String.valueOf(Boolean.TRUE), "--threshold", String.valueOf(threshold),
                    "--tempDir", getTempPath().toString()});
            mJobs.putAll(rowSimilarityJobs);

            if (hasOption("outputPathForSimilarityMatrix")) {
                Path outputPathForSimilarityMatrix = new Path(getOption("outputPathForSimilarityMatrix"));

                Job outputSimilarityMatrix = prepareJob(similarityMatrixPath, outputPathForSimilarityMatrix, SequenceFileInputFormat.class, ItemSimilarityJob.MostSimilarItemPairsMapper.class, EntityEntityWritable.class, DoubleWritable.class, ItemSimilarityJob.MostSimilarItemPairsReducer.class, EntityEntityWritable.class, DoubleWritable.class, TextOutputFormat.class);

                Configuration mostSimilarItemsConf = outputSimilarityMatrix.getConfiguration();
                mostSimilarItemsConf.set(ItemSimilarityJob.ITEM_ID_INDEX_PATH_STR, new Path(prepPath, "itemIDIndex").toString());

                mostSimilarItemsConf.setInt(ItemSimilarityJob.MAX_SIMILARITIES_PER_ITEM, maxSimilaritiesPerItem);
                /**
                 * depend on job --- asMatrix
                 */
                cOutputSimilarityMatrix = new ControlledJob(new Configuration());
                cOutputSimilarityMatrix.setJob(outputSimilarityMatrix);
                mJobs.put("outputSimilarityMatrix", cOutputSimilarityMatrix);
            }

        }

        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            Job partialMultiply = new Job(getConf(), "partialMultiply");
            Configuration partialMultiplyConf = partialMultiply.getConfiguration();

            MultipleInputs.addInputPath(partialMultiply, similarityMatrixPath, SequenceFileInputFormat.class, SimilarityMatrixRowWrapperMapper.class);

            MultipleInputs.addInputPath(partialMultiply, new Path(prepPath, "userVectors"), SequenceFileInputFormat.class, UserVectorSplitterMapper.class);

            partialMultiply.setJarByClass(ToVectorAndPrefReducer.class);
            partialMultiply.setMapOutputKeyClass(VarIntWritable.class);
            partialMultiply.setMapOutputValueClass(VectorOrPrefWritable.class);
            partialMultiply.setReducerClass(ToVectorAndPrefReducer.class);
            partialMultiply.setOutputFormatClass(SequenceFileOutputFormat.class);
            partialMultiply.setOutputKeyClass(VarIntWritable.class);
            partialMultiply.setOutputValueClass(VectorAndPrefsWritable.class);
            partialMultiplyConf.setBoolean("mapred.compress.map.output", true);
            partialMultiplyConf.set("mapred.output.dir", partialMultiplyPath.toString());

            if (usersFile != null) {
                partialMultiplyConf.set("usersFile", usersFile);
            }
            partialMultiplyConf.setInt("maxPrefsPerUserConsidered", maxPrefsPerUser);

            /**
             * depend on job --- asMatrix and toUserVectors
             */
            cPartialMultiply = new ControlledJob(new Configuration());
            cPartialMultiply.setJob(partialMultiply);
            mJobs.put("partialMultiply", cPartialMultiply);
        }

        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (filterFile != null) {
                Job itemFiltering = prepareJob(new Path(filterFile), explicitFilterPath, TextInputFormat.class, ItemFilterMapper.class, VarLongWritable.class, VarLongWritable.class, ItemFilterAsVectorAndPrefsReducer.class, VarIntWritable.class, VectorAndPrefsWritable.class, SequenceFileOutputFormat.class);
                /**
                 * no dependents but it is a no used job
                 */
                cItemFiltering = new ControlledJob(new Configuration());
                cItemFiltering.setJob(itemFiltering);
                mJobs.put("itemFiltering", cItemFiltering);
            }

            String aggregateAndRecommendInput = partialMultiplyPath.toString();
            if (filterFile != null) {
                aggregateAndRecommendInput = aggregateAndRecommendInput + "," + explicitFilterPath;
            }

            Job aggregateAndRecommend = prepareJob(new Path(aggregateAndRecommendInput), outputPath, SequenceFileInputFormat.class, PartialMultiplyMapper.class, VarLongWritable.class, PrefAndSimilarityColumnWritable.class, AggregateAndRecommendReducer.class, VarLongWritable.class, RecommendedItemsWritable.class, TextOutputFormat.class);

            Configuration aggregateAndRecommendConf = aggregateAndRecommend.getConfiguration();
            if (itemsFile != null) {
                aggregateAndRecommendConf.set("itemsFile", itemsFile);
            }

            if (filterFile != null) {
                setS3SafeCombinedInputPath(aggregateAndRecommend, getTempPath(), partialMultiplyPath, explicitFilterPath);
            }
            setIOSort(aggregateAndRecommend);
            aggregateAndRecommendConf.set("itemIDIndexPath", new Path(prepPath, "itemIDIndex").toString());

            aggregateAndRecommendConf.setInt("numRecommendations", numRecommendations);
            aggregateAndRecommendConf.setBoolean("booleanData", booleanData);
            /**
             * dependents on partialMultiply and itemFiltering (if it exists)
             */
            cAggregateAndRecommend = new ControlledJob(new Configuration());
            cAggregateAndRecommend.setJob(aggregateAndRecommend);
            if (null != cPartialMultiply) {
                cAggregateAndRecommend.addDependingJob(cPartialMultiply);
            }
            if (null != cItemFiltering) {
                cAggregateAndRecommend.addDependingJob(cItemFiltering);
            }
            mJobs.put("aggregateAndRecommend", cAggregateAndRecommend);
        }

        ControlledJob tmpJob;
        //set dependence: countObservations toItemVectors
        tmpJob = mJobs.get("normsAndTranspose");
        if (null != tmpJob) {
            tmpJob.addDependingJob(mJobs.get("toItemVectors"));
            mJobs.put("normsAndTranspose", tmpJob);
        }
        //set dependence: partialMultiply asMatrix+toUserVectors
        tmpJob = mJobs.get("partialMultiply");
        if (null != tmpJob) {
            tmpJob.addDependingJob(mJobs.get("asMatrix"));
            tmpJob.addDependingJob(mJobs.get("toUserVectors"));
            mJobs.put("partialMultiply", tmpJob);
        }
        //set dependence: outputPathForSimilarityMatrix
        tmpJob = mJobs.get("outputPathForSimilarityMatrix");
        if (null != tmpJob) {
            tmpJob.addDependingJob(mJobs.get("asMatrix"));
            mJobs.put("outputPathForSimilarityMatrix", tmpJob);
        }

        return mJobs;
    }

    public int run(String[] args) throws Exception {
        return 0;
    }

    private static void setIOSort(JobContext job) {
        Configuration conf = job.getConfiguration();
        conf.setInt("io.sort.factor", 100);
        String javaOpts = conf.get("mapred.map.child.java.opts");
        if (javaOpts == null) {
            javaOpts = conf.get("mapred.child.java.opts");
        }
        int assumedHeapSize = 512;
        if (javaOpts != null) {
            Matcher m = Pattern.compile("-Xmx([0-9]+)([mMgG])").matcher(javaOpts);
            if (m.find()) {
                assumedHeapSize = Integer.parseInt(m.group(1));
                String megabyteOrGigabyte = m.group(2);
                if ("g".equalsIgnoreCase(megabyteOrGigabyte)) {
                    assumedHeapSize *= 1024;
                }
            }
        }

        conf.setInt("io.sort.mb", Math.min(assumedHeapSize / 2, 1024));

        conf.setInt("mapred.task.timeout", 3600000);
    }

    public static long runJob(String[] args) throws Exception {
        return ToolRunner.run(new Configuration(), new RecommenderJob(), args);
    }
}