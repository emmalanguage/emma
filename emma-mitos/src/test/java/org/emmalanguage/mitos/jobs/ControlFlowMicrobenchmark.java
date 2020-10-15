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

package org.emmalanguage.mitos.jobs;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.emmalanguage.mitos.*;
import org.emmalanguage.mitos.operators.*;
import org.emmalanguage.mitos.partitioners.Always0;
import org.emmalanguage.mitos.partitioners.Forward;
import org.emmalanguage.mitos.partitioners.Random;
import org.emmalanguage.mitos.partitioners.RoundRobin;
import org.emmalanguage.mitos.util.TupleIntInt;
import org.emmalanguage.mitos.util.Unit;
import org.emmalanguage.mitos.util.Util;

import java.util.Arrays;

/**
 * // BB 0
 * i = 1
 * coll = <200 elements>
 * do {
 *     // BB 1
 *     i = i + 1
 *     coll.map{x => x + 1}
 * } while (i < 100)
 * // BB 2
 * assert i == 100
 */

public class ControlFlowMicrobenchmark {

	private static TypeInformation<ElementOrEvent<Integer>> integerTypeInfo = TypeInformation.of(new TypeHint<ElementOrEvent<Integer>>(){});
	private static TypeInformation<ElementOrEvent<Boolean>> booleanTypeInfo = TypeInformation.of(new TypeHint<ElementOrEvent<Boolean>>(){});
	private static TypeInformation<ElementOrEvent<Unit>> unitTypeInfo = TypeInformation.of(new TypeHint<ElementOrEvent<Unit>>(){});

	private static TypeSerializer<Integer> integerSer = TypeInformation.of(Integer.class).createSerializer(new ExecutionConfig());
	private static TypeSerializer<Boolean> booleanSer = TypeInformation.of(Boolean.class).createSerializer(new ExecutionConfig());

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//		Configuration cfg = new Configuration();
//		cfg.setLong("taskmanager.network.numberOfBuffers", 16384);
//		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(40, cfg);

		//env.getConfig().setParallelism(1);

		final int n = Integer.parseInt(args[0]);
		final int numElements = Integer.parseInt(args[1]);

		PojoTypeInfo.registerCustomSerializer(ElementOrEvent.class, new ElementOrEvent.ElementOrEventSerializerFactory());
		PojoTypeInfo.registerCustomSerializer(TupleIntInt.class, TupleIntInt.TupleIntIntSerializer.class);

		CFLConfig.getInstance().terminalBBId = 2;
		KickoffSource kickoffSrc = new KickoffSource(0,1);
		env.addSource(kickoffSrc).addSink(new DiscardingSink<>());


		Integer[] input = new Integer[]{1};

		LabySource<Integer> inputBag = new LabySource<>(env.fromCollection(Arrays.asList(input)), 0, integerTypeInfo);

		Integer[] input2 = new Integer[numElements];
		for (int i=0; i<numElements; i++) {
			input2[i] = 0;
		}

		LabySource<Integer> inputBag2 = new LabySource<>(env.fromCollection(Arrays.asList(input2)), 0, integerTypeInfo);

		LabyNode<Integer, Integer> inputBag2Partitioned =
				new LabyNode<>("inputBag2Partitioned", new IdMap<>(), 0, new RoundRobin<>(env.getParallelism()), integerSer, integerTypeInfo)
						.addInput(inputBag2, true);

		LabyNode<Integer, Integer> phi =
				LabyNode.phi("phi", 1, new Always0<>(1), integerSer, integerTypeInfo)
						.addInput(inputBag, false)
						.setParallelism(1);

		LabyNode<Integer, Integer> phi2 =
				LabyNode.phi("phi2", 1, new Forward<>(env.getParallelism()), integerSer, integerTypeInfo)
						.addInput(inputBag2Partitioned, false, true);

		LabyNode<Integer, Integer> inced =
				new LabyNode<>("inc-map", new IncMap(), 1, new Always0<>(1), integerSer, integerTypeInfo)
						.addInput(phi, true, false)
						.setParallelism(1);

		LabyNode<Integer, Integer> inced2 =
				new LabyNode<>("IncMapSynced", new IncMapSynced(), 1, new Forward<>(env.getParallelism()), integerSer, integerTypeInfo)
						.addInput(phi2, true, false);

		phi.addInput(inced, false, true);

		phi2.addInput(inced2, false, true);

		LabyNode<Integer, Boolean> smallerThan =
				new LabyNode<>("smaller-than", new SmallerThan(n), 1, new Always0<>(1), integerSer, booleanTypeInfo)
						.addInput(inced, true, false)
						.setParallelism(1);

		LabyNode<Boolean, Unit> exitCond =
				new LabyNode<>("exit-cond", new ConditionNode(1,2), 1, new Always0<>(1), booleanSer, unitTypeInfo)
						.addInput(smallerThan, true, false)
						.setParallelism(1);

//		LabyNode<Integer, Unit> assertEquals =
//				new LabyNode<>("Check i == " + n, new AssertEquals<>(n), 2, new Always0<>(1), integerSer, unitTypeInfo)
//					.addInput(inced, false, true)
//					.setParallelism(1);


		LabyNode.translateAll(env);

		System.out.println(env.getExecutionPlan());
		Util.executeWithCatch(env);
	}
}
