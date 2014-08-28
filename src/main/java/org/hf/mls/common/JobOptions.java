package org.hf.mls.common;

import org.apache.hadoop.io.compress.SnappyCodec;

/**
 * Created by He Fan on 2014/6/4.
 */
public class JobOptions {
    public static final String COMPRESS = "compress";

    public static final String UNCOMPRESS = "uncompress";

    public static final String TAB = "2";

    public static final String COMMA = "1";

    public static final Class COMPRESS_CLASS = SnappyCodec.class;

    //smaller is faster!
    public static final int OPTIMIZE = 1;
}
