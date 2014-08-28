package org.hf.mls.driver;

import org.hf.mls.ar.hadoop.FPGrowthJob;
import org.hf.mls.cf.hadoop.CFItemBaseJob;
import org.hf.mls.pref.hadoop.CalculatorJob;

/**
 * Created by He Fan on 2014/7/14.
 */
public class MLDriver {

    /**
     *
     * @param args
     * @return
     * @throws Throwable
     */
    public static int runCfJobs(String[] args) throws Throwable {
        return CFItemBaseJob.runJobs(args);
    }

    /**
     *
     * @param argString
     * @return
     * @throws Throwable
     */
    public static int runCfJobs(String argString) throws Throwable {
        String[] args = argString.split(" ");
        return CFItemBaseJob.runJobs(args);
    }

    /**
     *
     * @param args
     * @return
     * @throws Throwable
     */
    public static int runArJobs(String[] args) throws Throwable {
        FPGrowthJob rj = new FPGrowthJob();
        return rj.runJobs(args);
    }

    /**
     *
     * @param argString
     * @return
     * @throws Throwable
     */
    public static int runArJobs(String argString) throws Throwable {
        String[] args = argString.split(" ");
        FPGrowthJob rj = new FPGrowthJob();
        return rj.runJobs(args);
    }

    /**
     *
     * @param args
     * @return
     * @throws Throwable
     */
    public static int runPrefJobs(String[] args) throws Throwable {
        return CalculatorJob.runJobs(args);
    }

    /**
     *
     * @param argString
     * @return
     * @throws Throwable
     */
    public static int runPrefJobs(String argString) throws Throwable {
        String[] args = argString.split(" ");
        return CalculatorJob.runJobs(args);
    }
}
