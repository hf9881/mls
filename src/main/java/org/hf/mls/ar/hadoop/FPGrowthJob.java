package org.hf.mls.ar.hadoop;

import org.hf.mls.ar.driver.ArDriver;
import org.hf.mls.ar.hadoop.tohbase.ToHbaseJob;
import org.hf.mls.common.AbstractJob;
import org.hf.mls.common.JobOptions;
import org.hf.mls.common.Utils;
import org.hf.mls.mahout.driver.MahoutDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by He Fan on 2014/8/28.
 * <p/>
 * Usage: ArDriver
 * --InputDir
 * --batchId
 * --userIndex
 * --itemIndex
 * --splitChar
 * --minSupport
 * --numGroups
 * --tempDir
 */

public class FPGrowthJob extends AbstractJob {
    private static final String COMPRESS_OPTION = JobOptions.COMPRESS;

    private static final Logger LOGGER = LoggerFactory.getLogger(ArDriver.class);

    public static int runJobs(String[] args) throws Throwable {

        String commandChar = "--";

        addOption("inputDir", "", true);
        addOption("batchId", "-1", true);
        addOption("userIndex", "0", true);
        addOption("itemIndex", "1", true);
        addOption("splitChar", "\t", false);
        addOption("minSupport", "100", false);
        addOption("numGroups", "800", false);
        addOption("tempDir", "tmp/", true);

        if (!parseArguments(args, commandChar)) {
            return -1;
        }

        String inputDir = getOption("inputDir");
        String batchId = getOption("batchId");
        String userIndex = getOption("userIndex");
        String itemIndex = getOption("itemIndex");
        String splitChar = getOption("splitChar");
        String minSupport = getOption("minSupport");
        String numGroups = getOption("numGroups");
        String tempDir = getOption("tempDir");


        int percent = 0;
        int step = 100 / 8;

        String dir = tempDir + batchId;
        String preDir = dir + "/pre";
        String countDir = dir + "/tmp";
        String arDir = dir + "/ar";
        String mahoutTempDir = dir + "/mahoutTmpDir";
        String arOutputDir = Utils.TABLE_RECOMMEND_RESULTS;

        String evlOutputDir = dir + "/Evaluate";
        String preStaticOutputDir = dir + "/PreStatistics";

        String evalDir = Utils.TABLE_EVALUATION_CHECKLIST;
        String staticOutputDir = dir + "/Statistics";

        String staticDir = Utils.TABLE_STATISTICS_RESULTS;

        if (!Utils.rmHdfsDir(dir)) {
            return -1;
        }

        Long startTime = System.currentTimeMillis();
        printProgress(0, 0);

        // Extract data and Remove duplicates.
        if (0 == PreProcessJob.runJob(new String[]{
                inputDir,
                preDir,
                userIndex,
                itemIndex,
                splitChar,
                batchId})) {
            logJobFailure(PreProcessJob.class.getName());
            return -1;
        }
        logJobSuccess(PreProcessJob.class.getName());
        percent = printProgress(percent, step);

        // Count numbers of all
        long n = CountJob.getCount(new String[]{preDir, countDir, batchId});
        if (0 == n) {
            logJobFailure("Count is 0.");
            return -1;
        }
        logJobSuccess(CountJob.class.getName());
        percent = printProgress(percent, step);

        // Run shell to operate mahout fpg
        if (-1 == MahoutDriver.run(new String[]{
                "fpg",
                "-i", preDir,
                "-o", arDir,
                "-s", minSupport,
                "-g", numGroups,
                "--method", "mapreduce",
                "--tempDir", mahoutTempDir})) {
            logJobFailure("FPGrowth");
            return -1;
        }
        logJobSuccess("FPGrowth");
        percent = printProgress(percent, step);

        //Extract the ar out data
        if (0 == ExtractJob.extractData(new String[]{arDir, arOutputDir, batchId})) {
            logJobFailure(ExtractJob.class.getName());
            return -1;
        }
        logJobSuccess(ExtractJob.class.getName());
        percent = printProgress(percent, step);

        //Evaluation quotas job
        if (0 == EvaluateJob.getCount(new String[]{arDir, evlOutputDir, batchId, String.valueOf(n)})) {
            logJobFailure(EvaluateJob.class.getName());
            return -1;
        }
        logJobSuccess(EvaluateJob.class.getName());
        percent = printProgress(percent, step);

        //put evaluation list data to hbase table
        if (0 == ToHbaseJob.extractData(new String[]{
                evlOutputDir, evalDir, "quota_list", JobOptions.COMMA,
                batchId})) {
            logJobFailure("EvalList " + ToHbaseJob.class.getName());
            return -1;
        }
        logJobSuccess("EvalList " + ToHbaseJob.class.getName());
        percent = printProgress(percent, step);

        //Pre statistic data job
        if (0 == PreStaticJob.staticData(new String[]{
                evlOutputDir, preStaticOutputDir, COMPRESS_OPTION,
                batchId})) {
            logJobFailure(PreStaticJob.class.getName());
            return -1;
        }
        logJobSuccess(PreStaticJob.class.getName());
        percent = printProgress(percent, step);

        //Statistic
        if (0 == StaticJob.staticData(new String[]{
                preStaticOutputDir, staticOutputDir,
                batchId})) {
            logJobFailure(StaticJob.class.getName());
            return -1;
        }
        logJobSuccess(StaticJob.class.getName());
        percent = printProgress(percent, step);

        //put evaluation list data to hbase table
        if (0 == ToHbaseJob.extractData(new String[]{
                staticOutputDir, staticDir, "keys_list", JobOptions.TAB,
                batchId})) {
            logJobFailure("Static data" + ToHbaseJob.class.getName());
            return -1;
        }
        logJobSuccess("Static data" + ToHbaseJob.class.getName());
        printProgress(100, 0);

        //Complete!
        LOGGER.info("All jobs success!");
        LOGGER.info("Job cost time: " + (System.currentTimeMillis() - startTime) / 1000 + "s");

        if (!Utils.rmHdfsDir(dir)) {
            return -1;
        }

        return 0;
    }

    private static int printProgress(int p, int step) {
        p += step;
        LOGGER.info("progress " + p);
        return p;
    }

}
