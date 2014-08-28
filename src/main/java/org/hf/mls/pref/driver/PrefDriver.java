package org.hf.mls.pref.driver;

import org.hf.mls.pref.hadoop.CalculatorJob;

public class PrefDriver {

    public static void main(String[] args) throws Throwable {
        CalculatorJob.runJobs(args);
    }

}