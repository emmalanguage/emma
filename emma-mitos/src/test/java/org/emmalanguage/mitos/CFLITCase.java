/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.emmalanguage.mitos;

import eu.stratosphere.mitos.CFLManager;
import org.emmalanguage.mitos.jobs.ClickCountDiffsScala;
import org.emmalanguage.mitos.jobs.ControlFlowMicrobenchmark;
import org.emmalanguage.mitos.jobsold.NoCF;
import org.emmalanguage.mitos.jobsold.SimpleCF;
import org.emmalanguage.mitos.inputgen.ClickCountDiffsInputGen;
import org.emmalanguage.mitos.jobsold.ConnectedComponents;
import org.emmalanguage.mitos.jobsold.ConnectedComponentsMB;
import org.emmalanguage.mitos.jobsold.EmptyBags;
import org.emmalanguage.mitos.jobsold.SimpleCFDataSize;
import org.apache.commons.io.FileUtils;
import org.apache.flink.runtime.client.JobCancellationException;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Random;

public class CFLITCase {

    // Some of the tests expect JobCancellationException. These are those that have not been updated
    // to use Util.executeWithCatch.

    @Before
    public void clearLabyNodes() {
        LabyNode.labyNodes.clear();
    }

    @Test(expected=JobCancellationException.class)
    public void testNoCFOld() throws Exception {
        NoCF.main(null);
    }

    @Test()
    public void testNoCFNew() throws Exception {
        org.emmalanguage.mitos.jobs.NoCF.main(null);
    }

    @Test(expected=JobCancellationException.class)
    public void testEmptyBags() throws Exception {
        EmptyBags.main(null);
    }

    @Test(expected=JobCancellationException.class)
    public void testSimpleCFOld() throws Exception {
        SimpleCF.main(new String[]{"100"});
    }

    @Test()
    public void testSimpleCFNew() throws Exception {
        org.emmalanguage.mitos.jobs.SimpleCF.main(new String[]{"100"});
    }

    @Test(expected=JobCancellationException.class)
    public void testSimpleCFDataSize() throws Exception {
        SimpleCFDataSize.main(new String[]{"50", "500"});
    }

    @Test(expected=JobCancellationException.class)
    public void testConnectedComponents() throws Exception {
        ConnectedComponents.main(new String[]{});
    }

    @Test(expected=JobCancellationException.class)
    public void testConnectedComponentsMB() throws Exception {
        ConnectedComponentsMB.main(new String[]{});
    }

    private final int clickCountNumDays = 30;
    private final int[] exp = new int[]{1010, 1032, 981, 977, 978, 981, 988, 987, 958, 997, 985, 994, 1001, 987, 1007, 971, 960, 976, 1025, 1022, 971, 993, 997, 996, 1038, 985, 974, 999, 1020};

    private String setupClickCount() throws Exception {
        int clickCountSize = 100000;
        String clickCountBasePath = "/tmp/ClickCountITCase/";
        FileUtils.deleteQuietly(new File(clickCountBasePath));
        return ClickCountDiffsInputGen.generate(clickCountSize, clickCountNumDays, clickCountBasePath, new Random(1234), 0.01);
    }

    @Test()
    public void testClickCountDiffsMitos() throws Exception {
        String path = setupClickCount();
        org.emmalanguage.mitos.jobs.ClickCountDiffs.main(new String[]{path, Integer.toString(clickCountNumDays), "true"});
        ClickCountDiffsInputGen.checkOutput(path, ClickCountDiffsInputGen.outPrefLaby, clickCountNumDays, exp);
    }

    @Test()
    public void testClickCountDiffsFlinkSep() throws Exception {
        String path = setupClickCount();
        int flinkSepNumDays = clickCountNumDays/10;
        org.emmalanguage.mitos.jobsflinksep.ClickCountDiffs.main(new String[]{path, Integer.toString(flinkSepNumDays)});
        ClickCountDiffsInputGen.checkOutput(path, ClickCountDiffsInputGen.outPrefFlinkSep, flinkSepNumDays, exp);
    }

    @Test()
    public void testClickCountDiffsScala() throws Exception {
        String path = setupClickCount();

        boolean exceptionReceived = false;
        try {
            ClickCountDiffsScala.main(new String[]{path, Integer.toString(clickCountNumDays), "true"});
        } catch (JobCancellationException ex) {
            exceptionReceived = true;
        }
        if (!exceptionReceived) {
            throw new RuntimeException("testClickCountDiffs job failed");
        }

        int[] exp = new int[]{1010, 1032, 981, 977, 978, 981, 988, 987, 958, 997, 985, 994, 1001, 987, 1007, 971, 960, 976, 1025, 1022, 971, 993, 997, 996, 1038, 985, 974, 999, 1020};
        ClickCountDiffsInputGen.checkOutput(path, ClickCountDiffsInputGen.outPrefLaby, clickCountNumDays, exp);
    }

    @Test()
    public void testControlFlowMicrobenchmark() throws Exception {
        ControlFlowMicrobenchmark.main(new String[]{"100", "200"});
    }

    private final String ckpDir = "/tmp/mitos-checkpoints";

    @Test()
    public void testSnapshotWriting() throws Exception {
        CFLConfig cflConfig = CFLConfig.getInstance();
        try {
            cflConfig.shouldEnableCheckpointing = true;
            cflConfig.checkpointInterval = 10;
            cflConfig.checkpointDir = ckpDir;
            FileUtils.deleteQuietly(new File(cflConfig.checkpointDir));

            String path = setupClickCount();
            org.emmalanguage.mitos.jobs.ClickCountDiffsNoJoin.main(new String[]{path, Integer.toString(clickCountNumDays), "true"});

            //TODO: check:
            // - the program output
            // - there is a lastCompleteSnapshot file
            // - there are only complete snapshots
            // - all snapshots have the same number of files
            // - number of top-level dirs/files (includes number of snapshots)

        } finally {
            cflConfig.shouldEnableCheckpointing = false;
        }
    }

    @Test()
    public void testSnapshotRestore() throws Exception {
        CFLConfig cflConfig = CFLConfig.getInstance();
        try {
            cflConfig.shouldEnableCheckpointing = true;
            cflConfig.checkpointInterval = 10;
            cflConfig.checkpointDir = ckpDir;
            FileUtils.deleteQuietly(new File(cflConfig.checkpointDir));

            FileUtils.copyDirectory(new File("/home/gabor/Dropbox/mitos-checkpoints-torestore"), new File(cflConfig.checkpointDir));

            String path = setupClickCount();
            org.emmalanguage.mitos.jobs.ClickCountDiffsNoJoin.main(new String[]{path, Integer.toString(clickCountNumDays), "true"});

        } finally {
            cflConfig.shouldEnableCheckpointing = false;
        }
    }

//    @Test()
//    public void debug() throws Exception {
//        for(int i=0; i<100; i++) {
//            LabyNode.labyNodes.clear();
//            try {
//                //NoCF.main(null);
//                ConnectedComponentsMB.main(new String[]{});
//            } catch (JobCancellationException ex) {
//                //ok
//            }
//        }
//    }
}
