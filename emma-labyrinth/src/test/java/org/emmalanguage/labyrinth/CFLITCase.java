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

package org.emmalanguage.labyrinth;

import jobs.ClickCountDiffsScala;
import jobsold.NoCF;
import jobsold.SimpleCF;
import inputgen.ClickCountDiffsInputGen;
import jobsold.ConnectedComponents;
import jobsold.ConnectedComponentsMB;
import jobsold.EmptyBags;
import jobsold.SimpleCFDataSize;
import org.apache.commons.io.FileUtils;
import org.apache.flink.runtime.client.JobCancellationException;
import org.junit.Test;

import java.io.File;
import java.util.Random;

public class CFLITCase {

    @Test(expected=JobCancellationException.class)
    public void testNoCFOld() throws Exception {
        NoCF.main(null);
    }

    @Test(expected=JobCancellationException.class)
    public void testNoCFNew() throws Exception {
        LabyNode.labyNodes.clear();
        jobs.NoCF.main(null);
    }

    @Test(expected=JobCancellationException.class)
    public void testEmptyBags() throws Exception {
        EmptyBags.main(null);
    }

    @Test(expected=JobCancellationException.class)
    public void testSimpleCFOld() throws Exception {
        SimpleCF.main(new String[]{"100"});
    }

    @Test(expected=JobCancellationException.class)
    public void testSimpleCFNew() throws Exception {
        LabyNode.labyNodes.clear();
        jobs.SimpleCF.main(new String[]{"100"});
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

    @Test()
    public void testClickCountDiffs() throws Exception {
        LabyNode.labyNodes.clear();

        String path = "/tmp/ClickCountITCase/";
        FileUtils.deleteQuietly(new File(path));

        int size = 100000;
        int numDays = 30;

        path = ClickCountDiffsInputGen.generate(size, numDays, path, new Random(1234), 0.01);

        boolean exceptionReceived = false;
        try {
            ClickCountDiffsScala.main(new String[]{path, Integer.toString(numDays), "true"});
        } catch (JobCancellationException ex) {
            exceptionReceived = true;
        }
        if (!exceptionReceived) {
            throw new RuntimeException("testClickCountDiffs job failed");
        }

        int[] exp = new int[]{1010, 1032, 981, 977, 978, 981, 988, 987, 958, 997, 985, 994, 1001, 987, 1007, 971, 960, 976, 1025, 1022, 971, 993, 997, 996, 1038, 985, 974, 999, 1020};
        ClickCountDiffsInputGen.checkLabyOut(path, numDays, exp);

        int nocflNumDays = numDays/6;
        nolaby.ClickCountDiffs.main(new String[]{"/tmp/ClickCountITCase/" + size, Integer.toString(nocflNumDays)});
        ClickCountDiffsInputGen.checkNocflOut(path, nocflNumDays, exp);
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
