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

import org.emmalanguage.labyrinth.CFLConfig;
import org.emmalanguage.labyrinth.ElementOrEvent;
import org.emmalanguage.labyrinth.KickoffSource;
import org.emmalanguage.labyrinth.LabyNode;
import org.emmalanguage.labyrinth.LabySource;
import org.emmalanguage.labyrinth.operators.IdMap;
import org.emmalanguage.labyrinth.partitioners.Always0;
import org.emmalanguage.labyrinth.partitioners.RoundRobin;
import org.emmalanguage.labyrinth.util.Nothing;
import org.emmalanguage.labyrinth.operators.AssertBagEquals;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.emmalanguage.labyrinth.util.Util;

import java.util.Arrays;

public class NoCF {

	private static TypeSerializer<String> stringSer = TypeInformation.of(String.class).createSerializer(new ExecutionConfig());

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//		Configuration cfg = new Configuration();
//		cfg.setLong("taskmanager.network.numberOfBuffers", 16384);
//		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(100, cfg);

		//env.getConfig().setParallelism(1);

//		PojoTypeInfo.registerCustomSerializer(ElementOrEvent.class, new ElementOrEvent.ElementOrEventSerializerFactory());
//		PojoTypeInfo.registerCustomSerializer(TupleIntInt.class, TupleIntInt.TupleIntIntSerializer.class);

		CFLConfig.getInstance().terminalBBId = 0;
		KickoffSource kickoffSrc = new KickoffSource(0);
		env.addSource(kickoffSrc).addSink(new DiscardingSink<>());

		final int para = env.getParallelism();

		String[] words = new String[]{"alma", "korte", "alma", "b", "b", "b", "c", "d", "d"};


//		DataStream<ElementOrEvent<String>> input =
//				env.fromCollection(Arrays.asList(words))
//						.transform("bagify", Util.tpe(), new Bagify<>(new RoundRobin<>(para), 0))
//                        .setConnectionType(new FlinkPartitioner<>());

		LabySource<String> input = new LabySource<>(env.fromCollection(Arrays.asList(words)), 0, TypeInformation.of(new TypeHint<ElementOrEvent<String>>(){}));

		//System.out.println(input.getParallelism());

//		DataStream<ElementOrEvent<String>> output = input
//				.bt("id-map",input.getType(),
//				new BagOperatorHost<String, String>(new IdMap<>(), 0, 1, stringSer)
//						.addInput(0, 0, true, 0)
//						.out(0,0,true, new Always0<>(1)))
//				.setConnectionType(new FlinkPartitioner<>());

		LabyNode<String, String> output =
				new LabyNode<>("id-map", new IdMap<>(), 0, new RoundRobin<>(para), stringSer, TypeInformation.of(new TypeHint<ElementOrEvent<String>>(){}))
						.addInput(input, true);

//		output
//				.bt("assert", Util.tpe(), new BagOperatorHost<>(new AssertBagEquals<>("alma", "korte", "alma", "b", "b", "b", "c", "d", "d"), 0, 2, stringSer)
//						.addInput(0, 0, true, 1))
//				.setParallelism(1);

		LabyNode<String, Nothing> sink =
				new LabyNode<String, Nothing>(
						"assert",
						new AssertBagEquals<>("alma", "korte", "alma", "b", "b", "b", "c", "d", "d"),
						0,
						new Always0<String>(1), stringSer, TypeInformation.of(new TypeHint<ElementOrEvent<Nothing>>(){}))
						.addInput(output, true, false)
						.setParallelism(1);

		LabyNode.translateAll(env);

		//CFLConfig.getInstance().setNumToSubscribe(); //translateAll does this

		System.out.println(env.getExecutionPlan());
		//env.execute();
		Util.executeWithCatch(env);
	}
}