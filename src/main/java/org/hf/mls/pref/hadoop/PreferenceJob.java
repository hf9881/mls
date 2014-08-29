package org.hf.mls.pref.hadoop;

import org.hf.mls.common.AbstractJob;
import org.hf.mls.common.Utils;
import org.hf.mls.pref.driver.PrefDriver;
import org.hf.mls.pref.hadoop.entropy.EntropyPreJob;
import org.hf.mls.pref.hadoop.entropy.QuotaCalculateJob;
import org.hf.mls.pref.hadoop.tohbase.SeqToHbaseJob;
import org.hf.mls.pref.hadoop.zscore.StdDeviationPreJob;
import org.hf.mls.pref.hadoop.zscore.ZscoreJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by He Fan on 2014/8/28.
 * <p/>
 * Usage: PrefDriver
 * --InputDir
 * --batchId
 * --userIndex
 * --itemIndex
 * --splitChar
 * --prefsCount
 * --prefsIndexes
 * --tempDir
 * <p/>
 * not use now:
 * --stdMethodFlag
 * --hiveShellPath
 */
public class PreferenceJob extends AbstractJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(PrefDriver.class);

    private static final String DEFAULT_SPLIT_CHAR = "\t";

    public static int runJobs(String[] args) throws Throwable {

        addOption("InputDir", "", true);
        addOption("batchId", "", true);
        addOption("userIndex", "0", true);
        addOption("itemIndex", "1", true);
        addOption("splitChar", DEFAULT_SPLIT_CHAR, false);
        addOption("prefsIndexes", "2", true);
        addOption("tempDir", "tmp/", true);

        if (!parseArguments(args, "--")) {
            return -1;
        }

        String inputDir = getOption("InputDir");
        String batchId = getOption("batchId");
        String userIndex = getOption("userIndex");
        String itemIndex = getOption("itemIndex");
        String splitChar = getOption("splitChar");
        String prefsIndexes = getOption("prefsIndexes");
        String tempDir = getOption("tempDir");

        String prefsCount = String.valueOf(prefsIndexes.split("-").length);

        String dir = tempDir + batchId;
        String outputDir = dir + "/PreOut";
        String preHbaseDir = dir + "/PreHbase";
        String preStaticDir = dir + "/PreStatistics";
        String staticOutputDir = dir + "/Statistics";

        String hiveRecDir = dir + "/rec/";

        String prefSumPath = dir + "/prefSum/";
        String entropyPrePath = dir + "/entropy/";
        String stdDeviationPrePath = dir + "/stdDev/";

        String prefOutputDir = Utils.TABLE_RECOMMEND_RESULTS;
        String staticDir = Utils.TABLE_STATISTICS_RESULTS;

        int pc = Integer.parseInt(prefsCount);
        int percent = 0;
        int step = 100 / 8;

        double count = 0;
        double[] prefSum = new double[pc];
        double[] prefAverage = new double[pc];
        double[] stdDeviation = new double[pc];
        double[] entropy = new double[pc];
        double sumEntropy = 0;
        double[] weight = new double[pc];

        if (!Utils.rmHdfsDir(dir)) {
            return -1;
        }

        Long startTime = System.currentTimeMillis();
        printProgress(0, 0);

        //Extract the format of the pre data
        //Format Input:<u,i>
        //Format Output:<u,i1,i2...in>
        if (0 == PreProcessJob.runJob(new String[]{
                inputDir, outputDir, batchId, userIndex, itemIndex,
                splitChar, prefsCount, prefsIndexes})) {
            logJobFailure(PreProcessJob.class.getName());
            return -1;
        }
        logJobSuccess(PreProcessJob.class.getName());
        percent = printProgress(percent, step);

        count = QuotaCalculateJob.getSum(new String[]{
                outputDir, prefSumPath, batchId, prefsCount});
        if (-1 == count) {
            logJobFailure("Pref Sum");
            return -1;
        } else if (0 == count) {
            LOGGER.error("Pref Sum Job count value is 0!");
            return -1;
        }
        logJobSuccess("Pref Sum");
        percent = printProgress(percent, step);

        String prefSumStr = Utils.readHdfsFile(prefSumPath + "part-r-00000");
        LOGGER.info(prefSumStr);
        if ("null".equals(prefSumStr)) {
            return -1;
        }

        String[] qutoas = prefSumStr.split(",");

        StringBuilder prefAverStr = new StringBuilder("");
        for (int i = 0; i < pc; i++) {
            prefSum[i] = Double.parseDouble(qutoas[i]);
            prefAverage[i] = prefSum[i] / count;
            if (i > 0) {
                prefAverStr.append(",");
            }
            prefAverStr.append(String.valueOf(prefAverage[i]));
        }
        LOGGER.info("Prefs Sum:" + prefSumStr);
        LOGGER.info("Prefs Average:" + prefAverStr);

        String weightStr = "";
        if (1 == pc) {
            weightStr = "1";
        } else {
            //run Entropy Pre job
            if (0 == EntropyPreJob.runJob(new String[]{outputDir, entropyPrePath, batchId,
                    prefsCount, prefSumStr})) {
                logJobFailure(EntropyPreJob.class.getName());
                return -1;
            }
            logJobSuccess(EntropyPreJob.class.getName());
            percent = printProgress(percent, step);

            //get entropy array and sum Entropy
            String entropyStr = Utils.readHdfsFile(entropyPrePath + "part-r-00000");
            qutoas = entropyStr.split(",");
            LOGGER.info("f:" + entropyStr);

            double lnN = Math.log(count);
            for (int i = 0; i < pc; i++) {
                entropy[i] = Double.parseDouble(qutoas[i]) / lnN;
                sumEntropy += entropy[i];
                LOGGER.info("entropy:" + entropy[i]);
            }

            //calculate weight
            for (int i = 0; i < pc; i++) {
                weight[i] = (1 + entropy[i]) / (pc + sumEntropy);
                if (i > 0) {
                    weightStr += ",";
                }
                weightStr += String.valueOf(weight[i]);
            }
            LOGGER.info("weight:" + weight);
        }

        //run Std Deviation Pre job
        if (0 == StdDeviationPreJob.runJob(new String[]{outputDir, stdDeviationPrePath, batchId,
                prefsCount, prefAverStr.toString()})) {
            logJobFailure(StdDeviationPreJob.class.getName());
            return -1;
        }
        logJobSuccess(StdDeviationPreJob.class.getName());
        percent = printProgress(percent, step);

        String stdDevStr = Utils.readHdfsFile(stdDeviationPrePath + "part-r-00000");
        qutoas = stdDevStr.split(",");
        StringBuilder stdDevSb = new StringBuilder("");
        for (int i = 0; i < pc; i++) {
            stdDeviation[i] = Math.sqrt(Double.parseDouble(qutoas[i]) / count);
            if (i > 0) {
                stdDevSb.append(",");
            }
            stdDevSb.append(String.valueOf(stdDeviation[i]));
        }
        LOGGER.info("stdDeviation:" + stdDevSb);

        //run z-score job
        //run Std Deviation Pre job
        if (0 == ZscoreJob.runJob(new String[]{outputDir, hiveRecDir, batchId,
                prefsCount, prefAverStr.toString(), stdDevSb.toString(), weightStr})) {
            logJobFailure(ZscoreJob.class.getName());
            return -1;
        }
        logJobSuccess(ZscoreJob.class.getName());
        percent = printProgress(percent, step);

        //process pref list
        if (0 == PreHbaseJob.runJob(new String[]{hiveRecDir, preHbaseDir, batchId, "\t"})) {
            logJobFailure(PreHbaseJob.class.getName());
            return -1;
        }
        logJobSuccess(PreHbaseJob.class.getName());

        //put pref list to hbase
        if (0 == SeqToHbaseJob.runJob(new String[]{preHbaseDir, prefOutputDir, "item_list", batchId})) {
            logJobFailure("PrefToHbase");
            return -1;
        }
        logJobSuccess("PrefToHbase");
        percent = printProgress(percent, step);

        // Pre static the CF evaluation data
        //Format Input:<batchId_userId  itemId  preferValue>
        //Format Output:<batchId_01    itemId:用户数>
        if (0 == PreStaticJob.runJob(new String[]{hiveRecDir, preStaticDir, batchId, "\t"})) {
            logJobFailure(PreStaticJob.class.getName());
            return -1;
        }
        logJobSuccess(PreStaticJob.class.getName());
        percent = printProgress(percent, step);

        // Static evaluation data
        //Format Input:<batchId_01 itemId:用户数>
        //Format Output:<batchId_01/02  itemId1:用户数1,.../prefs1:用户数1,..>
        if (0 == StaticJob.runJob(new String[]{preStaticDir, staticOutputDir, batchId})) {
            logJobFailure(StaticJob.class.getName());
            return -1;
        }
        logJobSuccess(StaticJob.class.getName());
        percent = printProgress(percent, step);

        //put static list data to hbase table
        if (0 == SeqToHbaseJob.runJob(new String[]{staticOutputDir, staticDir, "keys_list", batchId})) {
            logJobFailure("EvalListToHbase");
            return -1;
        }
        logJobSuccess("EvalListToHbase");
        printProgress(100, 0);

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
