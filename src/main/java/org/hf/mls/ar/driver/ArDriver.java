package org.hf.mls.ar.driver;

import org.hf.mls.ar.hadoop.FPGrowthJob;

public class ArDriver {

    public static void main(String[] args) throws Throwable {
        FPGrowthJob rj = new FPGrowthJob();
        rj.runJobs(args);
    }
}