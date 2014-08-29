package org.hf.mls.driver;

import org.hf.mls.ar.hadoop.FPGrowthJob;
import org.hf.mls.cf.hadoop.CFItemBaseJob;
import org.hf.mls.pref.hadoop.PreferenceJob;

/**
 * Created by He Fan on 2014/7/14.
 */
public class MlsDriver {

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
        return FPGrowthJob.runJobs(args);
    }

    /**
     *
     * @param argString
     * @return
     * @throws Throwable
     */
    public static int runArJobs(String argString) throws Throwable {
        String[] args = argString.split(" ");
        return FPGrowthJob.runJobs(args);
    }

    /**
     *
     * @param args
     * @return
     * @throws Throwable
     */
    public static int runPrefJobs(String[] args) throws Throwable {
        return PreferenceJob.runJobs(args);
    }

    /**
     *
     * @param argString
     * @return
     * @throws Throwable
     */
    public static int runPrefJobs(String argString) throws Throwable {
        String[] args = argString.split(" ");
        return PreferenceJob.runJobs(args);
    }
}
