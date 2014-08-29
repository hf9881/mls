package org.hf.mls.pref.driver;

import org.hf.mls.pref.hadoop.PreferenceJob;

public class PrefDriver {

    public static void main(String[] args) throws Throwable {
        PreferenceJob.runJobs(args);
    }

}