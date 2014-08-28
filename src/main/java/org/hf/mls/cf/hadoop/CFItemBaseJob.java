package org.hf.mls.cf.hadoop;

import org.hf.mls.common.JobOptions;
import org.hf.mls.cf.driver.CfDriver;
import org.hf.mls.cf.hadoop.change.ChangeVectorToIntJob;
import org.hf.mls.cf.hadoop.change.ReduceNanJob;
import org.hf.mls.cf.hadoop.index.CreateDicJob;
import org.hf.mls.cf.hadoop.index.ReverseDicJob;
import org.hf.mls.cf.hadoop.pre.PreProcessJob;
import org.hf.mls.cf.hadoop.tohbase.ExtractJob;
import org.hf.mls.cf.hadoop.tohbase.SeqToHbaseJob;
import org.hf.mls.cf.hadoop.tohbase.TextToHbaseJob;
import org.hf.mls.common.AbstractJob;
import org.hf.mls.common.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by He Fan on 2014/8/28.
 * <p/>
 * Usage: CfDriver
 * --InputDir
 * --batchId
 * --userIndex
 * --itemIndex
 * --splitChar
 * --similarityClass
 * --numRecommendations
 * --maxSimilaritiesPerItem
 * --maxPrefsPerUserInItemSimilarity
 * --maxPrefsPerUser
 * --minPrefsPerUser
 * --threshold
 * --booleanData
 * --prefValueIndex
 * --userIdNan
 * --itemIdNan
 * --trainPercent
 * --tempDir
 * --optLevel
 */
public class CFItemBaseJob extends AbstractJob {
    private static final String COMPRESS_OPTION = JobOptions.COMPRESS;
    private static final Logger LOGGER = LoggerFactory.getLogger(CfDriver.class);

    public static int runJobs(String[] args) throws Throwable {
        addOption("Appid", "", true);
        addOption("InputDir", "", true);
        addOption("batchId", "-1", true);
        addOption("userIndex", "0", true);
        addOption("itemIndex", "1", true);
        addOption("splitChar", ",", false);
        addOption("similarityClass", "SIMILARITY_COOCCURRENCE", false);
        addOption("numRecommendations", "10", false);
        addOption("maxSimilaritiesPerItem", "100", false);
        addOption("maxPrefsPerUserInItemSimilarity", "500", false);
        addOption("maxPrefsPerUser", "500", false);
        addOption("minPrefsPerUser", "1", false);
        addOption("threshold", "4.9E-324D", false);
        addOption("booleanData", "false", false);
        addOption("prefValueIndex", "2", false);
        addOption("userIdNan", "false", false);
        addOption("itemIdNan", "false", false);
        addOption("trainPercent", "0.65", false);
        addOption("tempDir", "tmp/", false);
        addOption("optLevel", String.valueOf(JobOptions.OPTIMIZE), false);

        if (!parseArguments(args)) {
            return -1;
        }

        String APP_ID = getOption("Appid");
        String inputDir = getOption("InputDir");
        String batchId = getOption("batchId");
        String userIndex = getOption("userIndex");
        String itemIndex = getOption("itemIndex");
        String splitChar = getOption("splitChar");
        String similarityClass = getOption("similarityClass");
        String numRecommendations = getOption("numRecommendations");
        String maxSimilaritiesPerItem = getOption("maxSimilaritiesPerItem");
        String maxPrefsPerUserInItemSimilarity = getOption("maxPrefsPerUserInItemSimilarity");
        String maxPrefsPerUser = getOption("maxPrefsPerUser");
        String minPrefsPerUser = getOption("minPrefsPerUser");
        String threshold = getOption("threshold");
        String booleanData = getOption("booleanData");
        String prefValueIndex = getOption("prefValueIndex");
        String userIdNan = getOption("userIdNan");
        String itemIdNan = getOption("itemIdNan");
        String trainPercent = getOption("trainPercent");
        String tempDir = getOption("tempDir");
        String optLevel = getOption("optLevel");

        boolean isUserIdNan = userIdNan.equals("true");
        boolean isItemIdNan = itemIdNan.endsWith("true");
        int optimizeLevel = Integer.parseInt(optLevel);
        int percent = 0;
        int step = 100 / 32;

        String dir = tempDir + batchId;

        String userIdDic = dir + "/userDic";
        String itemIdDic = dir + "/itemDic";
        String userIdReverseDic = dir + "/userReDic";
        String itemIdReverseDic = dir + "/itemReDic";

        String userIdNumeric = dir + "/userNumeric";
        String itemIdNumeric = dir + "/itemNumeric";

        String extractTmp = dir + "/extractTmp";
        String userIdReduction = dir + "/userReduction";
        String itemIdReduction = dir + "/itemReduction";

        String numericDir = dir + "/numeric";

        String mahoutTempDir = dir + "/mahoutTmpDir";//mahout tmp dir
        String mahoutTempDir2 = dir + "/mahoutTmpDir2";//mahout tmp dir

        String preDir = dir + "/pre";//pre job output dir

        String cfDir = dir + "/cf";//mahout cf out put dir
        String cfResultTable = APP_ID + "." + Utils.TABLE_RECOMMEND_RESULTS;

        String trainAndTestSetDir = dir + "/ttSet";
        String testSet = "testSet/testSet";
        String testSetDir = trainAndTestSetDir + "/testSet";
        String trainSet = "trainSet/trainSet";
        String trainSetDir = trainAndTestSetDir + "/trainSet";

        String cfTrainOutputDir = dir + "/evaCf";//mahout cf evaluation out put dir
        String trainOutputDir = dir + "/evaOut";//process cf evaluation data output dir

        String nrsDir = dir + "/Nrs";//the number of items which both system recommended and user liked

        String evlOutputDir = dir + "/Evaluate";
        String evalListTable = APP_ID + "." + Utils.TABLE_EVALUATION_CHECKLIST;

        String preStaticDir = dir + "/PreStatistics";
        String staticInfoTable = APP_ID + "." + Utils.TABLE_STATISTICS_RESULTS;

        String inputPath = "";

        if (!Utils.rmHdfsDir(dir)) {
            return -1;
        }

        Long startTime = System.currentTimeMillis();
        percent = printProgress(percent, 0);

        //PreProcess the original data
        LOGGER.info("PreProcess job started!");
        long pre = PreProcessJob.runJob(new String[]{
                inputDir,
                preDir,
                userIndex,
                itemIndex,
                splitChar,
                batchId,
                booleanData,
                prefValueIndex,
                COMPRESS_OPTION});
        if (0 == pre) {
            logJobFailure(PreProcessJob.class.getName());
            return -1;
        } else {
            logJobSuccess(PreProcessJob.class.getName());
            percent = printProgress(percent, step);

        }

        JobControl myController0 = null;
        if (isItemIdNan || isUserIdNan) {
            myController0 = new JobControl("My CF job controller0");

            ControlledJob createItemDic = new ControlledJob(new Configuration());
            ControlledJob reverseItemDic = new ControlledJob(new Configuration());
            ControlledJob changeItemVector = new ControlledJob(new Configuration());
            if (isItemIdNan) {
                //Create the Item <-> Index map dic
                //Format Input:<U,I_nan(,p)>
                //Format Output:<,I_nan,_I_id>
                createItemDic.setJob(
                        CreateDicJob.init(new String[]{preDir, itemIdDic, batchId, "1",
                                COMPRESS_OPTION})
                );
                myController0.addJob(createItemDic);

                //Reverse the Item <-> Index map dic
                //Format Input:<,I_nan,_I_id>
                //Format Output:<,_I_id,I_nan>
                reverseItemDic.setJob(
                        ReverseDicJob.init(new String[]{itemIdDic, itemIdReverseDic, batchId, "1",
                                COMPRESS_OPTION})
                );
                reverseItemDic.addDependingJob(createItemDic);
                myController0.addJob(reverseItemDic);

                //Change the Item from nan to int
                //Format Input:<U,I_nan (,P)> + <,I_nan,_I_id>
                //Format Output:<U,_I_id (,P)>>
                changeItemVector.setJob(
                        ChangeVectorToIntJob.init(new String[]{preDir, itemIdDic, itemIdNumeric,
                                batchId, "1",
                                COMPRESS_OPTION})
                );
                switch (optimizeLevel) {
                    case 0:
                        changeItemVector.addDependingJob(createItemDic);
                        break;
                    case 1:
                        changeItemVector.addDependingJob(reverseItemDic);
                        break;
                    default:
                        changeItemVector.addDependingJob(createItemDic);
                }
                myController0.addJob(changeItemVector);
            }

            if (isUserIdNan) {
                //Create the User <-> Index map dic
                //Format Input:<U_nan,I(,p)>
                //Format Output:<U_nan,U_num>
                ControlledJob createUserDic = new ControlledJob(new Configuration());
                createUserDic.setJob(
                        CreateDicJob.init(new String[]{preDir, userIdDic, batchId, "0",
                                COMPRESS_OPTION})
                );
                switch (optimizeLevel) {
                    case 0:
                        //DO Nothing...
                        break;
                    case 1:
                        createUserDic.addDependingJob(createItemDic);
                        break;
                    case 2:
                        createUserDic.addDependingJob(changeItemVector);
                        break;
                    default:
                        changeItemVector.addDependingJob(changeItemVector);
                }
                myController0.addJob(createUserDic);

                //Reverse the User <-> Index map dic
                //Format Input:<U_nan,U_num>
                //Format Output:<U_num,U_nan>
                ControlledJob reverseUserDic = new ControlledJob(new Configuration());
                reverseUserDic.setJob(
                        ReverseDicJob.init(new String[]{userIdDic, userIdReverseDic, batchId, "0",
                                COMPRESS_OPTION})
                );
                reverseUserDic.addDependingJob(createUserDic);
                myController0.addJob(reverseUserDic);

                //Change the Item from nan to int
                //Format Input:<U,I_nan (,P)> + <,I_nan,_I_id>
                //Format Output:<U,_I_id (,P)>>
                ControlledJob changeUserVector = new ControlledJob(new Configuration());
                if (isItemIdNan) {
                    inputPath = itemIdNumeric;
                    changeUserVector.addDependingJob(changeItemVector);
                } else {
                    inputPath = preDir;
                }
                changeUserVector.setJob(
                        ChangeVectorToIntJob.init(new String[]{inputPath, userIdDic, userIdNumeric,
                                batchId, "0",
                                COMPRESS_OPTION})
                );
                switch (optimizeLevel) {
                    case 0:
                        changeUserVector.addDependingJob(createUserDic);
                        break;
                    case 1:
                        changeUserVector.addDependingJob(reverseUserDic);
                        break;
                    default:
                        changeUserVector.addDependingJob(reverseUserDic);
                }
                myController0.addJob(changeUserVector);
            }
        }

        JobControl myController1 = new JobControl("My CF job controller1");

        //Extract the format of the pre data
        //Format Input:<u,i(,p)>
        //Format Output:<u,i1(\t p),i2...in>
        if (isUserIdNan) {
            inputPath = userIdNumeric;
        } else if (isItemIdNan) {
            inputPath = itemIdNumeric;
        } else {
            inputPath = preDir;
        }
        ControlledJob reFormatJob = new ControlledJob(new Configuration());
        reFormatJob.setJob(ReFormatJob.init(new String[]{inputPath, trainAndTestSetDir,
                COMPRESS_OPTION, trainPercent,
                batchId, booleanData,
                trainSet, testSet}));
        myController1.addJob(reFormatJob);


        //Run mahout Cf recommender of evaluation
        //Format Input:<flag,u,i1,i2...in>
        //Format Output:<u,Nr,i1,i2...inr>
        org.hf.mls.mahout.cf.taste.hadoop.item.RecommenderJob rj = new org.hf.mls.mahout.cf.taste.hadoop.item.RecommenderJob();
        Map<String, ControlledJob> mahoutEvalJobs = rj.getJobs(new String[]{
                "--input", trainSetDir,
                "--output", cfTrainOutputDir,
                "--similarityClassname", similarityClass,
                "--booleanData", booleanData,
                "--numRecommendations", numRecommendations,
                "--maxSimilaritiesPerItem", maxSimilaritiesPerItem,
                "--maxPrefsPerUserInItemSimilarity", maxPrefsPerUserInItemSimilarity,
                "--maxPrefsPerUser", maxPrefsPerUser,
                "--minPrefsPerUser", minPrefsPerUser,
                "--threshold", threshold,
                "--tempDir", mahoutTempDir});
        ControlledJob tmpJob;
        tmpJob = mahoutEvalJobs.get("itemIDIndex");
        tmpJob.addDependingJob(reFormatJob);
        mahoutEvalJobs.put("itemIDIndex", tmpJob);

        switch (optimizeLevel) {
            case 0:
                tmpJob = mahoutEvalJobs.get("toUserVectors");
                tmpJob.addDependingJob(reFormatJob);
                mahoutEvalJobs.put("toUserVectors", tmpJob);
                break;
            case 1:
                tmpJob = mahoutEvalJobs.get("toUserVectors");
                tmpJob.addDependingJob(mahoutEvalJobs.get("itemIDIndex"));
                mahoutEvalJobs.put("toUserVectors", tmpJob);
                break;
            default:
                tmpJob = mahoutEvalJobs.get("toUserVectors");
                tmpJob.addDependingJob(mahoutEvalJobs.get("itemIDIndex"));
                mahoutEvalJobs.put("toUserVectors", tmpJob);

                tmpJob = mahoutEvalJobs.get("aggregateAndRecommend");
                tmpJob.addDependingJob(mahoutEvalJobs.get("itemFiltering"));
                mahoutEvalJobs.put("aggregateAndRecommend", tmpJob);
                break;
        }

        //Extract data from cf-evaluation output data set
        //Format Input:<mahout format>
        //Format Output:<u \t Ns,i1,i2...ins>
        ControlledJob extractEvalJob = new ControlledJob(new Configuration());
        extractEvalJob.setJob(ExtractEvalJob.init(new String[]{
                cfTrainOutputDir, trainOutputDir, "Nul", COMPRESS_OPTION,
                batchId}));
        extractEvalJob.addDependingJob(mahoutEvalJobs.get("aggregateAndRecommend"));
        myController1.addJob(extractEvalJob);

        //calculate Nrs(the number of items which system recommended) from cf-eva data set
        //Format Input:<u,Ns,i1,i2...ins> + <u,Nr,i1,i2...inr>
        //Format Output:<u,Nrs,Nr,Ns>
        ControlledJob calculateNrsJob = new ControlledJob(new Configuration());
        calculateNrsJob.setJob(CalculateNrsJob.init(new String[]{
                testSetDir, trainOutputDir, nrsDir, COMPRESS_OPTION,
                batchId}));
        calculateNrsJob.addDependingJob(extractEvalJob);
        myController1.addJob(calculateNrsJob);

        //calculate Precision and Recall
        //Format Input:<u,Nrs,Nr,Ns>
        //Format Output:<batch_id, u,Precision,Recall>
        ControlledJob evaluateJob = new ControlledJob(new Configuration());
        evaluateJob.setJob(EvaluateJob.init(new String[]{nrsDir, evlOutputDir, batchId, COMPRESS_OPTION}));
        evaluateJob.addDependingJob(calculateNrsJob);
        myController1.addJob(evaluateJob);

        //put evaluation list data to hbase table
        ControlledJob textToHbaseJob = new ControlledJob(new Configuration());
        textToHbaseJob.setJob(
                TextToHbaseJob.init(new String[]{evlOutputDir, evalListTable,
                        batchId, "quota_list"})
        );
        textToHbaseJob.addDependingJob(evaluateJob);
        myController1.addJob(textToHbaseJob);

        //Pre static the cf evaluation data
        //Format Input:<u,Precision,Recall>
        //Format Output:<batch_id + _01/02  P/R,用户数>
        ControlledJob preStaticJob = new ControlledJob(new Configuration());
        preStaticJob.setJob(
                StaticEvalJob.init(new String[]{
                        evlOutputDir, preStaticDir, batchId, COMPRESS_OPTION}));
        preStaticJob.addDependingJob(evaluateJob);
        myController1.addJob(preStaticJob);

        //put evaluation list data to hbase table
        ControlledJob seqToHbaseJob = new ControlledJob(new Configuration());
        seqToHbaseJob.setJob(
                SeqToHbaseJob.init(new String[]{
                        preStaticDir, staticInfoTable,
                        batchId, "keys_list:quota_pairs:quota_mean"})
        );
        seqToHbaseJob.addDependingJob(preStaticJob);
        myController1.addJob(seqToHbaseJob);

        //myController1.addJobCollection(mahoutEvalJobs.values());

        for (ControlledJob cj : mahoutEvalJobs.values()) {
            myController1.addJob(cj);
        }

        JobControl myController2 = new JobControl("My CF job controller2");

        //Run mahout cf recommender
        org.hf.mls.mahout.cf.taste.hadoop.item.RecommenderJob rj2 = new org.hf.mls.mahout.cf.taste.hadoop.item.RecommenderJob();
        if (isUserIdNan) {
            inputPath = userIdNumeric;
        } else if (isItemIdNan) {
            inputPath = itemIdNumeric;
        } else {
            inputPath = preDir;
        }
        Map<String, ControlledJob> mahoutRecJobs = rj2.getJobs(new String[]{
                        "--input", inputPath,
                        "--output", cfDir,
                        "--similarityClassname", similarityClass,
                        "--booleanData", booleanData,
                        "--numRecommendations", numRecommendations,
                        "--maxSimilaritiesPerItem", maxSimilaritiesPerItem,
                        "--maxPrefsPerUserInItemSimilarity", maxPrefsPerUserInItemSimilarity,
                        "--maxPrefsPerUser", maxPrefsPerUser,
                        "--minPrefsPerUser", minPrefsPerUser,
                        "--threshold", threshold,
                        "--tempDir", mahoutTempDir2}
        );
        switch (optimizeLevel) {
            case 0:
                //Do Nothing...
                break;
            case 1:
                tmpJob = mahoutRecJobs.get("toUserVectors");
                tmpJob.addDependingJob(mahoutRecJobs.get("itemIDIndex"));
                mahoutRecJobs.put("toUserVectors", tmpJob);
                break;
            default:
                tmpJob = mahoutRecJobs.get("toUserVectors");
                tmpJob.addDependingJob(mahoutRecJobs.get("itemIDIndex"));
                mahoutRecJobs.put("toUserVectors", tmpJob);

                tmpJob = mahoutRecJobs.get("aggregateAndRecommend");
                tmpJob.addDependingJob(mahoutRecJobs.get("itemFiltering"));
                mahoutRecJobs.put("aggregateAndRecommend", tmpJob);
                break;
        }

        ControlledJob extractJob = new ControlledJob(new Configuration());
        if (!isItemIdNan && !isUserIdNan) {
            //Extract data from cf output data set
            //Format Input:<mahout format>
            //Format Output:<batch_id + u   i1,i2...> to hbase
            extractJob.setJob(
                    ExtractJob.init(new String[]{cfDir, cfResultTable, batchId})
            );
            extractJob.addDependingJob(mahoutRecJobs.get("aggregateAndRecommend"));
            myController2.addJob(extractJob);
            for (ControlledJob cj : mahoutRecJobs.values()) {
                myController2.addJob(cj);
            }
        } else {
            //Extract tmp data from mahout output
            //Format Input:<mahout format>
            //Format Output:<U,I>
            extractJob.setJob(
                    ExtractTmpJob.init(new String[]{cfDir, extractTmp, batchId,
                            COMPRESS_OPTION})
            );
            extractJob.addDependingJob(mahoutRecJobs.get("aggregateAndRecommend"));
            myController2.addJob(extractJob);
            for (ControlledJob cj : mahoutRecJobs.values()) {
                myController2.addJob(cj);
            }

            ControlledJob reduceItemNan = new ControlledJob(new Configuration());
            if (isItemIdNan) {
                //Reduce the Item Numeric data to nan format
                //Format Input:<U,I_num>
                //Format Output:<U,I_nan>
                reduceItemNan.setJob(
                        ReduceNanJob.init(new String[]{extractTmp, itemIdReverseDic, itemIdReduction,
                                batchId, "1",
                                COMPRESS_OPTION})
                );
                reduceItemNan.addDependingJob(extractJob);
                myController2.addJob(reduceItemNan);
            }

            ControlledJob reduceUserNan = new ControlledJob(new Configuration());
            ControlledJob extractNumeric = new ControlledJob(new Configuration());
            if (isUserIdNan) {
                //Reduce the User Numeric data to nan format
                //Format Input:<U_num,I>
                //Format Output:<U_nan,I>
                if (isItemIdNan) {
                    inputPath = itemIdReduction;
                    reduceUserNan.addDependingJob(reduceItemNan);
                } else {
                    inputPath = extractTmp;
                    reduceUserNan.addDependingJob(extractJob);
                }
                reduceUserNan.setJob(
                        ReduceNanJob.init(new String[]{inputPath, userIdReverseDic, userIdReduction,
                                batchId, "0",
                                COMPRESS_OPTION})
                );
                myController2.addJob(reduceUserNan);
                inputPath = userIdReduction;
            } else {
                //Reduce the Numeric data extracted by ReduceNanJob(Item step only)
                extractNumeric.setJob(
                        ExtractNumericJob.init(new String[]{itemIdReduction, numericDir,
                                batchId, COMPRESS_OPTION})
                );
                extractNumeric.addDependingJob(reduceItemNan);
                myController2.addJob(extractNumeric);
                inputPath = numericDir;
            }

            //Put final data to hbase
            ControlledJob textToHbase = new ControlledJob(new Configuration());
            textToHbase.setJob(
                    TextToHbaseJob.init(new String[]{inputPath, cfResultTable,
                            batchId, "item_list"})
            );
            if (isUserIdNan) {
                textToHbase.addDependingJob(reduceUserNan);
            } else {
                textToHbase.addDependingJob(extractNumeric);
            }
            myController2.addJob(textToHbase);
        }

        percent = printProgress(percent, step);

        //run all jobs
        int completedJobCounts0 = 0;
        int completedJobCounts2 = 0;
        int completedJobCounts1 = 0;
        int tmp = 0;

        if (null != myController0) {
            Thread theController0 = new Thread(myController0);
            theController0.start();
            while (true) {
                if (myController0.allFinished()) {
                    myController0.stop();
                    LOGGER.info("Phase zero finished!");
                    break;
                } else {
                    tmp = myController0.getSuccessfulJobList().size();
                    if (tmp > completedJobCounts0) {
                        percent = printProgress(percent, step * (tmp - completedJobCounts0));
                        completedJobCounts0 = tmp;
                    }
                }
                Thread.sleep(4000);
            }

            List<ControlledJob> failedJobs0 = new ArrayList<ControlledJob>();
            failedJobs0.addAll(myController0.getFailedJobList());
            if (0 != failedJobs0.size()) {
                LOGGER.error("Phase zero Failed jobs:");
                for (ControlledJob j : failedJobs0) {
                    LOGGER.error(j.getJob().getJobName());
                }
                LOGGER.error("---------------------------------");
                return -1;
            }
        }

        Thread theController2 = new Thread(myController2);
        theController2.start();
        Thread theController1 = new Thread(myController1);
        theController1.start();

        boolean mc2Finished = true;
        boolean mc1Finished = true;

        while (true) {
            if (myController2.allFinished() && mc2Finished) {
                myController2.stop();
                mc2Finished = false;
                LOGGER.info("Phase two finished!");
            } else {
                tmp = myController2.getSuccessfulJobList().size();
                if (tmp > completedJobCounts2) {
                    percent = printProgress(percent, step * (tmp - completedJobCounts2));
                    completedJobCounts2 = tmp;
                }
            }
            if (myController1.allFinished() && mc1Finished) {
                myController1.stop();
                mc1Finished = false;
                LOGGER.info("Phase one finished!");
            } else {
                tmp = myController1.getSuccessfulJobList().size();
                if (tmp > completedJobCounts1) {
                    percent = printProgress(percent, step * (tmp - completedJobCounts1));
                    completedJobCounts1 = tmp;
                }
            }
            if (!mc1Finished && !mc2Finished) {
                break;
            }
            Thread.sleep(4000);
        }

        if (!Utils.rmHdfsDir(dir)) {
            return -1;
        }

        List<ControlledJob> failedJobs2 = new ArrayList<ControlledJob>();
        failedJobs2.addAll(myController2.getFailedJobList());
        List<ControlledJob> failedJobs1 = new ArrayList<ControlledJob>();
        failedJobs1.addAll(myController1.getFailedJobList());
        if (0 == failedJobs2.size() && 0 == failedJobs1.size()) {
            printProgress(100, 0);
            LOGGER.info("All jobs success!");
            LOGGER.info("Job cost time:" + (System.currentTimeMillis() - startTime) / 1000 + "s");
            return 0;
        } else if (0 != failedJobs1.size()) {
            LOGGER.error("Phase one Failed jobs:");
            for (ControlledJob j : failedJobs1) {
                LOGGER.error(j.getJob().getJobName());
            }
            LOGGER.error("---------------------------------");
        } else if (0 != failedJobs2.size()) {
            LOGGER.error("Phase two Failed jobs:");
            for (ControlledJob j : failedJobs2) {
                LOGGER.error(j.getJob().getJobName());
            }
            LOGGER.error("---------------------------------");
        }
        return -1;
    }

    private static int printProgress(int p, int step) {
        p += step;
        LOGGER.info("progress " + p);
        return p;
    }
}
