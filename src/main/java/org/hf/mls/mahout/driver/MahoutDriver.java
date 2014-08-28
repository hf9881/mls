package org.hf.mls.mahout.driver;

/**
 * Created by He Fan on 2014/6/18.
 */


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import org.apache.hadoop.util.ProgramDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public final class MahoutDriver {
    private static final Logger log = LoggerFactory.getLogger(MahoutDriver.class);

    public static int run(String[] args) throws Throwable {
        Properties mainClasses = loadProperties("driver.classes.props");
        if (mainClasses == null) {
            mainClasses = loadProperties("driver.classes.default.props");
        }
        if (mainClasses == null) {
            throw new IOException("Can't load any properties file?");
        }

        boolean foundShortName = false;
        ProgramDriver programDriver = new ProgramDriver();
        for (Iterator i$ = mainClasses.keySet().iterator(); i$.hasNext(); ) {
            Object key = i$.next();
            String keyString = (String) key;
            if ((args.length > 0) && (shortName(mainClasses.getProperty(keyString)).equals(args[0]))) {
                foundShortName = true;
            }
            if ((args.length > 0) && (keyString.equalsIgnoreCase(args[0])) && (isDeprecated(mainClasses, keyString))) {
                log.error(desc(mainClasses.getProperty(keyString)));
                return -1;
            }
            if (isDeprecated(mainClasses, keyString)) {
                continue;
            }
            addClass(programDriver, keyString, mainClasses.getProperty(keyString));
        }

        if ((args.length < 1) || (args[0] == null) || ("-h".equals(args[0])) || ("--help".equals(args[0]))) {
            programDriver.driver(args);
            return -1;
        }

        String progName = args[0];
        if (!foundShortName) {
            addClass(programDriver, progName, progName);
        }
        shift(args);

        Properties mainProps = loadProperties(progName + ".props");
        if (mainProps == null) {
            log.warn("No {}.props found on classpath, will use command-line arguments only", progName);
            mainProps = new Properties();
        }

        Map<String, Object[]> argMap = Maps.newHashMap();
        int i = 0;
        while ((i < args.length) && (args[i] != null)) {
            List argValues = Lists.newArrayList();
            String arg = args[i];
            i++;
            if (arg.startsWith("-D")) {
                String[] argSplit = arg.split("=");
                arg = argSplit[0];
                if (argSplit.length == 2)
                    argValues.add(argSplit[1]);
            } else {
                while ((i < args.length) && (args[i] != null) &&
                        (!args[i].startsWith("-"))) {
                    argValues.add(args[i]);
                    i++;
                }
            }
            argMap.put(arg, argValues.toArray(new String[argValues.size()]));
        }

        for (String key : mainProps.stringPropertyNames()) {
            String[] argNamePair = key.split("\\|");
            String shortArg = '-' + argNamePair[0].trim();
            String longArg = "--" + argNamePair[1].trim();
            if ((!argMap.containsKey(shortArg)) && ((longArg == null) || (!argMap.containsKey(longArg)))) {
                argMap.put(longArg, new String[]{mainProps.getProperty(key)});
            }

        }

        List argsList = Lists.newArrayList();
        argsList.add(progName);
        for (Map.Entry entry : argMap.entrySet()) {
            String arg = (String) entry.getKey();
            if (arg.startsWith("-D")) {
                String[] argValues = (String[]) entry.getValue();
                if ((argValues.length > 0) && (!argValues[0].trim().isEmpty())) {
                    arg = arg + '=' + argValues[0].trim();
                }
                argsList.add(1, arg);
            } else {
                argsList.add(arg);
                for (String argValue : Arrays.asList((String[]) argMap.get(arg))) {
                    if (!argValue.isEmpty()) {
                        argsList.add(argValue);
                    }
                }
            }
        }

        long start = System.currentTimeMillis();

        programDriver.driver((String[]) argsList.toArray(new String[argsList.size()]));

        if (log.isInfoEnabled()) {
            log.info("Program took {} ms (Minutes: {})",
                    Long.valueOf(System.currentTimeMillis() - start),
                    Double.valueOf((System.currentTimeMillis() - start) / 60000.0D));
        }
        return 0;
    }

    private static boolean isDeprecated(Properties mainClasses, String keyString) {
        return "deprecated".equalsIgnoreCase(shortName(mainClasses.getProperty(keyString)));
    }

    private static Properties loadProperties(String resource) throws IOException {
        InputStream propsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
        if (propsStream != null) {
            try {
                Properties properties = new Properties();
                properties.load(propsStream);
                Properties localProperties1 = properties;
                return localProperties1;
            } finally {
                Closeables.close(propsStream, true);
            }
        }
        return null;
    }

    private static String[] shift(String[] args) {
        System.arraycopy(args, 1, args, 0, args.length - 1);
        args[(args.length - 1)] = null;
        return args;
    }

    private static String shortName(String valueString) {
        return valueString.contains(":") ? valueString.substring(0, valueString.indexOf(58)).trim() : valueString;
    }

    private static String desc(String valueString) {
        return valueString.contains(":") ? valueString.substring(valueString.indexOf(58)).trim() : valueString;
    }

    private static void addClass(ProgramDriver driver, String classString, String descString) {
        try {
            Class clazz = Class.forName(classString);
            driver.addClass(shortName(descString), clazz, desc(descString));
        } catch (ClassNotFoundException e) {
            log.warn("Unable to add class: {}", classString, e);
        } catch (Throwable t) {
            log.warn("Unable to add class: {}", classString, t);
        }
    }
}