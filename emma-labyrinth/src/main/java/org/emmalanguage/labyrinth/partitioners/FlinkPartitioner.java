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

package org.emmalanguage.labyrinth.partitioners;

import org.emmalanguage.labyrinth.ElementOrEvent;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * This just looks into the event to tell the result chosen by our manual partitioner to Flink.
 */
public class FlinkPartitioner<T> extends StreamPartitioner<ElementOrEvent<T>> {

    public static final short BROADCAST = -1;

    private final int[] arr = new int[1];
    private int[] broadcastArr = null;

    @Override
    public StreamPartitioner<ElementOrEvent<T>> copy() {
        return new FlinkPartitioner<T>();
    }

    @Override
    public int[] selectChannels(SerializationDelegate<StreamRecord<ElementOrEvent<T>>> record, int numChannels) {
        short targetPart = record.getInstance().getValue().targetPart;
        if (targetPart != BROADCAST) {
            arr[0] = targetPart;
            return arr;
        } else {
            return broadcast(numChannels);
        }
    }

    private int[] broadcast(int numChannels) {
        if (broadcastArr == null) {
            broadcastArr = new int[numChannels];
            for (int i=0; i<numChannels; i++) {
                broadcastArr[i] = i;
            }
        }
        assert broadcastArr.length == numChannels;
        return broadcastArr;
    }

    @Override
    public String toString() {
        return "FlinkPartitioner";
    }
}
