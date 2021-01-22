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

import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;

public class CFLConfig implements Serializable {
    private static final CFLConfig sing = new CFLConfig();

    public static CFLConfig getInstance() {
        return sing;
    }

    private CFLConfig() {}

    // This has to be set before creating the KickoffSource, which takes and stores it in the constructor.
    // Furthermore it has to be set when the job starts (i.e. it can't be set during KickoffSource setup)
    // because it is needed during BagOperatorHost setup and the order of the setups is not deterministic.
    public int terminalBBId = -1;

    public int numToSubscribe = -1;

    public void setNumToSubscribe() {
        int totalPara = 0;
        for (DataStream<?> ds: DataStream.btStreams) {
            totalPara += ds.getParallelism();
        }
        DataStream.btStreams.clear();
        setNumToSubscribe(totalPara);
    }

    public void setNumToSubscribe(int totalPara) {
        this.numToSubscribe = totalPara;
    }


    public boolean reuseInputs = true;


    public boolean shouldEnableCheckpointing = false; // propagates to CFLManager only if true
    public static final int checkpointIntervalNotSet = -27;
    public int checkpointInterval = checkpointIntervalNotSet; // propagates to CFLManager only if set
    public String checkpointDir = null; // propagates to CFLManager only if not null


    public static final boolean vlog = false;

    public static final boolean logStartEnd = false;
}
