package org.hf.mls.driver;

import org.hf.mls.ar.hadoop.FPGrowthJob;
import org.hf.mls.cf.hadoop.CFItemBaseJob;
import org.hf.mls.pref.hadoop.PreferenceJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by He Fan on 2014/7/14.
 */
public class MlsDriver {

    private static final Logger LOGGER = LoggerFactory.getLogger(MlsDriver.class);
    private static final String errMsg = "Usage: Method(AR or CF or PREF) arguments...";

    public static void main(String[] args) throws Throwable {
        if (0 == args.length) {
            LOGGER.error(errMsg);
        } else {
            StringBuilder newArgs = new StringBuilder("");
            boolean first = true;
            for (String arg : args) {
                if (first) {
                    first = false;
                    continue;
                }
                newArgs.append(arg).append(" ");
            }

//            String[] subArgs = new String[]{};
//            for (int i = 0; i < args.length - 1; i++) {
//                subArgs[i] = args[i + 1];
//            }

            if ("AR".equals(args[0])) {
                runArJobs(newArgs.toString());
            } else if ("CF".equals(args[0])) {
                runCfJobs(newArgs.toString());
            } else if ("PREF".equals(args[0])) {
                runPrefJobs(newArgs.toString());
            } else {
                LOGGER.error(errMsg);
            }
        }


    }

    /**
     * @param args
     * @return
     * @throws Throwable
     */
    public static int runCfJobs(String[] args) throws Throwable {
        return CFItemBaseJob.runJobs(args);
    }

    /**
     * @param argString
     * @return
     * @throws Throwable
     */
    public static int runCfJobs(String argString) throws Throwable {
        String[] args = argString.split(" ");
        return CFItemBaseJob.runJobs(args);
    }

    /**
     * @param args
     * @return
     * @throws Throwable
     */
    public static int runArJobs(String[] args) throws Throwable {
        return FPGrowthJob.runJobs(args);
    }

    /**
     * @param argString
     * @return
     * @throws Throwable
     */
    public static int runArJobs(String argString) throws Throwable {
        String[] args = argString.split(" ");
        return FPGrowthJob.runJobs(args);
    }

    /**
     * @param args
     * @return
     * @throws Throwable
     */
    public static int runPrefJobs(String[] args) throws Throwable {
        return PreferenceJob.runJobs(args);
    }

    /**
     * @param argString
     * @return
     * @throws Throwable
     */
    public static int runPrefJobs(String argString) throws Throwable {
        String[] args = argString.split(" ");
        return PreferenceJob.runJobs(args);
    }
}
