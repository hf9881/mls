/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hf.mls.mahout.cf.taste.hadoop.item;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.hadoop.EntityPrefWritable;
import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;

/**
 * <h1>Input</h1>
 * <p/>
 * <p>
 * Takes user IDs as {@link org.apache.mahout.math.VarLongWritable} mapped to all associated item IDs and preference values, as
 * {@link org.apache.mahout.cf.taste.hadoop.EntityPrefWritable}s.
 * </p>
 * <p/>
 * <h1>Output</h1>
 * <p/>
 * <p>
 * The same user ID mapped to a {@link org.apache.mahout.math.RandomAccessSparseVector} representation of the same item IDs and
 * preference values. Item IDs are used as vector indexes; they are hashed into ints to work as indexes with
 * {@link org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils#idToIndex(long)}. The mapping is remembered for later with a combination of
 * {@link org.apache.mahout.cf.taste.hadoop.item.ItemIDIndexMapper} and {@link org.apache.mahout.cf.taste.hadoop.item.ItemIDIndexReducer}.
 * </p>
 */
public final class ToUserVectorsReducer extends
        Reducer<VarLongWritable, VarLongWritable, VarLongWritable, VectorWritable> {

    public static final String MIN_PREFERENCES_PER_USER = ToUserVectorsReducer.class.getName()
            + ".minPreferencesPerUser";

    private int minPreferences;

    public enum Counters {USERS}

    private final VectorWritable userVectorWritable = new VectorWritable();

    @Override
    protected void setup(Context ctx) throws IOException, InterruptedException {
        super.setup(ctx);
        minPreferences = ctx.getConfiguration().getInt(MIN_PREFERENCES_PER_USER, 1);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        //we need the number of users later
        Configuration conf = context.getConfiguration();
        int numberOfUsers = (int) context.getCounter(Counters.USERS).getValue();
        HadoopUtil.writeInt(numberOfUsers, new Path(conf.get("NUM_USERS", "")), conf);
    }

    @Override
    protected void reduce(VarLongWritable userID,
                          Iterable<VarLongWritable> itemPrefs,
                          Context context) throws IOException, InterruptedException {
        Vector userVector = new RandomAccessSparseVector(Integer.MAX_VALUE, 100);
        for (VarLongWritable itemPref : itemPrefs) {
            int index = TasteHadoopUtils.idToIndex(itemPref.get());
            float value = itemPref instanceof EntityPrefWritable ? ((EntityPrefWritable) itemPref).getPrefValue() : 1.0f;
            userVector.set(index, value);
        }

        if (userVector.getNumNondefaultElements() >= minPreferences) {
            userVectorWritable.set(userVector);
            userVectorWritable.setWritesLaxPrecision(true);
            context.getCounter(Counters.USERS).increment(1);
            context.write(userID, userVectorWritable);
        }
    }

}
