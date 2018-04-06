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

package org.emmalanguage.labyrinth.jobs;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.io.TupleCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.emmalanguage.labyrinth.*;
import org.emmalanguage.labyrinth.operators.*;
import org.emmalanguage.labyrinth.partitioners.*;
import org.emmalanguage.labyrinth.util.TupleIntInt;
import org.emmalanguage.labyrinth.util.Unit;

import java.util.Collections;

public class ClickCountDiffsNoJoin {

    private static TupleTypeInfo<Tuple2<Integer, Integer>> typeInfoTupleIntInt = new TupleTypeInfo<>(TypeInformation.of(Integer.class), TypeInformation.of(Integer.class));

    private static TypeSerializer<Integer> integerSer = TypeInformation.of(Integer.class).createSerializer(new ExecutionConfig());
    private static TypeSerializer<Boolean> booleanSer = TypeInformation.of(Boolean.class).createSerializer(new ExecutionConfig());
    private static TypeSerializer<TupleIntInt> tupleIntIntSer = new TupleIntInt.TupleIntIntSerializer();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //env.setParallelism(1);

        final String pref = args[0] + "/";


        PojoTypeInfo.registerCustomSerializer(ElementOrEvent.class, new ElementOrEvent.ElementOrEventSerializerFactory());
        PojoTypeInfo.registerCustomSerializer(TupleIntInt.class, TupleIntInt.TupleIntIntSerializer.class);


        CFLConfig.getInstance().terminalBBId = 4;
        KickoffSource kickoffSrc = new KickoffSource(0, 1);
        env.addSource(kickoffSrc).addSink(new DiscardingSink<>());

        final int para = env.getParallelism();


        // BB 0

        @SuppressWarnings("unchecked")
        LabySource<TupleIntInt> yesterdayCounts_1 =
                new LabySource<>(env.fromCollection(Collections.emptyList(), TypeInformation.of(TupleIntInt.class)), 0, TypeInformation.of(new TypeHint<ElementOrEvent<TupleIntInt>>(){}));

        LabySource<Integer> day_1 =
                new LabySource<>(env.fromCollection(Collections.singletonList(1)), 0, TypeInformation.of(new TypeHint<ElementOrEvent<Integer>>(){}))
                        .setParallelism(1);

        // -- Iteration starts here --   BB 1

        LabyNode<TupleIntInt, TupleIntInt> yesterdayCounts_2 =
                LabyNode.phi("yesterdayCounts_2", 1, new Forward<>(para), tupleIntIntSer, TypeInformation.of(new TypeHint<ElementOrEvent<TupleIntInt>>(){}))
                .addInput(yesterdayCounts_1, false);

        LabyNode<Integer, Integer> day_2 =
                LabyNode.phi("day_2", 1, new Always0<>(1), integerSer, TypeInformation.of(new TypeHint<ElementOrEvent<Integer>>(){}))
                .addInput(day_1, false)
                .setParallelism(1);

        LabyNode<Integer, Integer> visits_1 =
                new LabyNode<>("visits_1", new ClickLogReader(pref + "in/clickLog_"), 1, new RoundRobin<>(para), integerSer, TypeInformation.of(new TypeHint<ElementOrEvent<Integer>>(){}))
                        .addInput(day_2, true, false);
                        //.setParallelism(1);

        LabyNode<Integer, TupleIntInt> visitsMapped =
                new LabyNode<>("visitsMapped", new FlatMap<Integer, TupleIntInt>() {
                    @Override
                    public void pushInElement(Integer e, int logicalInputId) {
                        super.pushInElement(e, logicalInputId);
                        out.collectElement(TupleIntInt.of(e, 1));
                    }
                }, 1, new IntegerBy0(para), integerSer, TypeInformation.of(new TypeHint<ElementOrEvent<TupleIntInt>>(){}))
                .addInput(visits_1, true, false);

        LabyNode<TupleIntInt, TupleIntInt> counts =
                new LabyNode<>("counts", new GroupBy0Sum1TupleIntInt(), 1, new TupleIntIntBy0(para), tupleIntIntSer, TypeInformation.of(new TypeHint<ElementOrEvent<TupleIntInt>>(){}))
                .addInput(visitsMapped, true, false);

        LabyNode<Integer, Boolean> notFirstDay =
                new LabyNode<>("notFirstDay", new SingletonBagOperator<Integer, Boolean>() {
                    @Override
                    public void pushInElement(Integer e, int logicalInputId) {
                        super.pushInElement(e, logicalInputId);
                        out.collectElement(!e.equals(1));
                    }
                }, 1, new Always0<>(1), integerSer, TypeInformation.of(new TypeHint<ElementOrEvent<Boolean>>(){}))
                .addInput(day_2, true, false)
                .setParallelism(1);

        LabyNode<Boolean, Unit> ifCond =
                new LabyNode<>("ifCond", new ConditionNode(new int[]{2,3}, new int[]{3}), 1, new Always0<>(1), booleanSer, TypeInformation.of(new TypeHint<ElementOrEvent<Unit>>(){}))
                .addInput(notFirstDay, true, false)
                .setParallelism(1);

        // -- then branch   BB 2

        // The join of joinedYesterday is merged into this operator
        LabyNode<TupleIntInt, Integer> diffs =
                new LabyNode<>("diffs", new OuterJoinTupleIntInt<Integer>() {
                    @Override
                    protected void inner(int b, TupleIntInt p) {
                        out.collectElement(Math.abs(b - p.f1));
                    }

                    @Override
                    protected void right(TupleIntInt p) {
                        out.collectElement(p.f1);
                    }

                    @Override
                    protected void left(int b) {
                        out.collectElement(b);
                    }
                }, 2, new TupleIntIntBy0(para), tupleIntIntSer, TypeInformation.of(new TypeHint<ElementOrEvent<Integer>>(){}))
                .addInput(yesterdayCounts_2, false, true)
                .addInput(counts, false, true);

        LabyNode<Integer, Integer> sumCombiner =
                new LabyNode<>("sumCombiner", new SumCombiner(), 2, new Forward<>(para), integerSer, TypeInformation.of(new TypeHint<ElementOrEvent<Integer>>(){}))
                .addInput(diffs, true, false);

        LabyNode<Integer, Integer> sum =
                new LabyNode<>("sum", new Sum(), 2, new Always0<>(1), integerSer, TypeInformation.of(new TypeHint<ElementOrEvent<Integer>>(){}))
                .addInput(sumCombiner, true, false)
                .setParallelism(1);

        LabyNode<Integer, Unit> printSum =
                new LabyNode<>("printSum", new CFAwareFileSink(pref + "out_nojoin/diff_"), 2, new Always0<>(1), integerSer, TypeInformation.of(new TypeHint<ElementOrEvent<Unit>>(){}))
                .addInput(day_2, false, true)
                .addInput(sum, true, false)
                .setParallelism(1);

        // -- end of then branch   BB 3

        // (We "optimize away" yesterdayCounts_3, since it would be an IdMap)
        yesterdayCounts_2.addInput(counts, false, true);

        LabyNode<Integer, Integer> day_3 =
                new LabyNode<>("day_3", new IncMap(), 3, new Always0<>(1), integerSer, TypeInformation.of(new TypeHint<ElementOrEvent<Integer>>(){}))
                .addInput(day_2, false, false)
                .setParallelism(1);

        day_2.addInput(day_3, false, true);

        LabyNode<Integer, Boolean> notLastDay =
                new LabyNode<>("notLastDay", new SmallerThan(Integer.parseInt(args[1]) + 1), 3, new Always0<>(1), integerSer, TypeInformation.of(new TypeHint<ElementOrEvent<Boolean>>(){}))
                .addInput(day_3, true, false)
                .setParallelism(1);

        LabyNode<Boolean, Unit> exitCond =
                new LabyNode<>("exitCond", new ConditionNode(1, 4), 3, new Always0<>(1), booleanSer, TypeInformation.of(new TypeHint<ElementOrEvent<Unit>>(){}))
                .addInput(notLastDay, true, false)
                .setParallelism(1);

        // -- Iteration ends here   BB 4

        // Itt nincs semmi operator. (A kiirast a BB 2-ben csinaljuk.)

        LabyNode.translateAll();

        env.execute();
    }
}
