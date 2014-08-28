package org.hf.mls.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;

/**
 * Created by He Fan on 2014/7/15.
 */
public class Utils {
    public static final String TABLE_RECOMMEND_RESULTS = "Recsys_recommend_results";
    public static final String TABLE_EVALUATION_CHECKLIST = "Recsys_evaluation_checklist";
    public static final String TABLE_STATISTICS_RESULTS = "Recsys_statistics_results";

    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

    public static final boolean rmHdfsDir(String path) throws Throwable {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        Path p = new Path(path);
        try {
            if (hdfs.exists(p)) {
                hdfs.delete(p, true);
                LOGGER.info("Delete dir:" + p.toString());
            }
        } catch (Exception e) {
            LOGGER.error("Delete tmp hdfs files failed!", e);
            return false;
        }
        return true;
    }

    public static final boolean rmHdfsDir(String[] paths) throws Throwable {
        boolean success = false;
        for (String path : paths) {
            success = rmHdfsDir(path);
        }
        return success;
    }

    public static final String readHdfsFile(String path) throws Throwable {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        InputStream in = null;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        String str = "null";
        try {
            in = hdfs.open(new Path(path));
            IOUtils.copyBytes(in, out, 2048, false);
            byte[] buffer = out.toByteArray();
            str = new String(buffer);
        } finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
            return str.replace("\t", "").replace("\n", "");
        }
    }


    public static int mkHdfsDir(String path) throws Throwable {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path(path);
        boolean success = fs.mkdirs(srcPath);
        fs.close();
        if (success) {
            return 0;
        } else {
            return -1;
        }
    }
}
