package org.hf.mls.cf.driver;

import org.hf.mls.cf.hadoop.CFItemBaseJob;
import org.hf.mls.common.AbstractJob;

/**
 * Created by He Fan on 2014/5/30.
 */
public class CfDriver extends AbstractJob {

    public static void main(String[] args) throws Throwable {
        CFItemBaseJob rj = new CFItemBaseJob();
        rj.runJobs(args);
    }
}