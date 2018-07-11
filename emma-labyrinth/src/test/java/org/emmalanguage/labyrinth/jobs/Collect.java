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
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.runtime.client.JobCancellationException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.emmalanguage.labyrinth.*;
import org.emmalanguage.labyrinth.operators.AssertBagEquals;
import org.emmalanguage.labyrinth.operators.IdMap;
import org.emmalanguage.labyrinth.partitioners.Always0;
import org.emmalanguage.labyrinth.partitioners.RoundRobin;
import org.emmalanguage.labyrinth.util.Nothing;
import org.emmalanguage.labyrinth.util.SocketCollector;
import org.emmalanguage.labyrinth.util.Util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Consumer;

public class Collect {

	private static TypeSerializer<String> stringSer = TypeInformation.of(String.class).createSerializer(new ExecutionConfig());

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		PojoTypeInfo.registerCustomSerializer(ElementOrEvent.class, new ElementOrEvent.ElementOrEventSerializerFactory());

		CFLConfig.getInstance().terminalBBId = 0;
		KickoffSource kickoffSrc = new KickoffSource(0);
		env.addSource(kickoffSrc).addSink(new DiscardingSink<>());

		final int para = env.getParallelism();

		String[] words = new String[]{"alma", "korte", "alma", "b", "b", "b", "c", "d", "d"};


		LabySource<String> input = new LabySource<>(env.fromCollection(Arrays.asList(words)), 0, TypeInformation.of(new TypeHint<ElementOrEvent<String>>(){}));

		LabyNode<String, String> output =
				new LabyNode<>("id-map", new IdMap<>(), 0, new RoundRobin<>(para), stringSer, TypeInformation.of(new TypeHint<ElementOrEvent<String>>(){}))
						.addInput(input, true);

		SocketCollector<String> socColl = Util.collect(env, output, 0);

		LabyNode.translateAll(env);

		System.out.println(env.getExecutionPlan());

		ArrayList<String> collected = Util.executeAndGetCollected(env, socColl);
		for(String s: collected) {
			System.out.println(s);
		}
	}
}