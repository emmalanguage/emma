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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.emmalanguage.labyrinth.operators.Bagify;
import org.emmalanguage.labyrinth.util.Util;
import org.emmalanguage.labyrinth.partitioners.FlinkPartitioner;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class LabySource<T> extends AbstractLabyNode<T> {

    public final int bbId;
    public final int opID;

    private final DataStream<T> inputStream;

    public final Bagify<T> bagify;

    private SingleOutputStreamOperator<ElementOrEvent<T>> flinkStream;

    private int parallelism = -1;

    private final TypeInformation<ElementOrEvent<T>> typeInfo;

    public LabySource(DataStream<T> inputStream, int bbId, TypeInformation<ElementOrEvent<T>> typeInfo) {
        assert bbId == 0; // because Bagify always sends only once
        this.bbId = bbId;
        this.opID = labyNodes.size();
        this.inputStream = inputStream;
        this.typeInfo = typeInfo;
        bagify = new Bagify<>(null, opID);
        labyNodes.add(this);
    }

    public LabySource<T> setParallelism(int parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    @Override
    public DataStream<ElementOrEvent<T>> getFlinkStream() {
        return flinkStream;
    }

    @Override
    protected void translate(StreamExecutionEnvironment env) {
        flinkStream = inputStream
                .transform("bagify", Util.tpe(), bagify)
                .returns(typeInfo);

        if (parallelism != -1) {
            flinkStream = flinkStream.setParallelism(parallelism);
        }

        flinkStream = flinkStream.setConnectionType(new FlinkPartitioner<>());
    }
}
