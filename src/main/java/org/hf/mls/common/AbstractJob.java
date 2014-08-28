package org.hf.mls.common;

/**
 * Created by He Fan on 2014/8/28.
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public abstract class AbstractJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractJob.class);
    private static Map<String, String> argMapDefault = new HashMap<String, String>();
    private static List<String> argListRequired = new ArrayList<String>();
    private static Map<String, String> argMapOptions;

    protected AbstractJob() {
    }

    protected static boolean parseArguments(String[] args) {
        OptionsHelper optionHelper = new OptionsHelper(args);
        StringBuilder argsString = new StringBuilder("Arguments: ");

        argMapOptions = optionHelper.getOptionsPairs();
        if (null == argMapOptions) {
            LOGGER.error("Arguments missing!");
            return false;
        }
        for (String name : argListRequired) {
            if (!argMapOptions.containsKey(name)) {
                LOGGER.error("Necessary arguments missing!");
                return false;
            }
        }

        for (String name : argMapOptions.keySet()) {
            argsString.append("--").append(name).append("[").append(argMapOptions.get(name)).append("] ");
        }

        for(String name :argMapDefault.keySet()){
            if(!argMapOptions.containsKey(name)){
                argsString.append("--").append(name).append("(").append(argMapDefault.get(name)).append(") ");
            }
        }
        LOGGER.info(argsString.toString());
        return true;
    }

    protected static void addOption(String name, String defaultValue, boolean required) {
        argMapDefault.put(name, defaultValue);
        if (required) {
            argListRequired.add(name);
        }
    }

    protected static String getOption(String name) {
        return argMapOptions.containsKey(name) ? argMapOptions.get(name) : argMapDefault.get(name);
    }

    protected static void logJobSuccess(String jobName){
        LOGGER.info(jobName + " Job success! ");
    }

    protected static void logJobFailure(String jobName){
        LOGGER.error(jobName + " Job failed! ");
    }
}
