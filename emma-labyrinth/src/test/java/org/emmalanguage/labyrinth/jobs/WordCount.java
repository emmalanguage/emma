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

import org.emmalanguage.labyrinth.*;
import org.emmalanguage.labyrinth.partitioners.Always0;
import org.emmalanguage.labyrinth.partitioners.RoundRobin;
import org.emmalanguage.labyrinth.util.Unit;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.Collector;
import org.emmalanguage.labyrinth.operators.FlatMap;
import org.emmalanguage.labyrinth.operators.GroupByString0Count1;
import org.emmalanguage.labyrinth.operators.Print;

import java.util.Arrays;

public class WordCount {

	private static TypeSerializer<String> stringSerializer = TypeInformation
			.of(String.class)
			.createSerializer(new ExecutionConfig());

	private static TypeSerializer<Tuple2<String, Integer>> tuple2StringIntegerSerializer = TypeInformation
			.of(new TypeHint<Tuple2<String, Integer>>() {})
			.createSerializer(new ExecutionConfig());

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		CFLConfig.getInstance().terminalBBId = 0; // this will have to be found automatically
		KickoffSource kickoffSource = new KickoffSource(0); // this as well
		env.addSource(kickoffSource)
				.addSink(new DiscardingSink<>()); // kickoff source has no output

		final int para = env.getParallelism();

		String[] lines = {"foo bar foo foo bar lol lol lol foo rofl",
				" lol foo lol bar lol bar bar foo foo rofl foo",
				"foo bar foo foo bar lol lol lol foo rofl lasagne"};


		// source to read line by line
		LabySource<String> input = new LabySource<>(env.fromCollection(Arrays.asList(lines)),
				0,
				TypeInformation.of(new TypeHint<ElementOrEvent<String>>() {}));

		// split lines
//		LabyNode<String, String> split = new LabyNode<>(
//				"split-map",
//				new SplitLineAtSpaceMap(),
//				0,
//				new RoundRobin<>(para),
//				stringSerializer,
//				TypeInformation.of(new TypeHint<ElementOrEvent<String>>(){})
//		)
//				.addInput(input, true)
//				.setParallelism(para);

		LabyNode<String, String> split =  new LabyNode<>(
				"split2",
				FlatMap.create((String s, Collector<String> col) -> {
					for (String elem : s.split(" ")) { col.collect(elem); } }),
				0,
				new RoundRobin<>(para),
				stringSerializer,
				TypeInformation.of(new TypeHint<ElementOrEvent<String>>(){})
		)
				.addInput(input, true)
				.setParallelism(para);

		// map phase
//		LabyNode<String, Tuple2<String, Integer>> mapnode = new LabyNode<>(
//				"map-phase",
//				new WordToWord1TupleMap(),
//				0,
//				new RoundRobin<>(para),
//				stringSerializer,
//				TypeInformation.of(new TypeHint<ElementOrEvent<Tuple2<String, Integer>>>(){})
//		)
//				.addInput(split, true, false)
//				.setParallelism(para);

		LabyNode<String, Tuple2<String, Integer>> mapnode = new LabyNode<>(
				"map-phase",
				FlatMap.create((String s, Collector<Tuple2<String, Integer>> col) ->
						col.collect(new Tuple2<String, Integer>(s, 1))),
				0,
				new RoundRobin<>(para),
				stringSerializer,
				TypeInformation.of(new TypeHint<ElementOrEvent<Tuple2<String, Integer>>>(){})
		)
				.addInput(split, true, false)
				.setParallelism(para);

		// count phase
		LabyNode<Tuple2<String, Integer>, Tuple2<String, Integer>> reduceNode = new LabyNode<>(
				"reduce-phase",
				new GroupByString0Count1(),
				0,
				new Always0<>(para),
				tuple2StringIntegerSerializer,
				TypeInformation.of(new TypeHint<ElementOrEvent<Tuple2<String, Integer>>>(){})
		)
				.addInput(mapnode, true, false)
				.setParallelism(para);

		LabyNode<Tuple2<String, Integer>, Unit> printNode = new LabyNode<> (
				"print-phase",
				new Print<>("printcount"),
				0,
				new Always0<>(1),
				tuple2StringIntegerSerializer,
				TypeInformation.of(new TypeHint<ElementOrEvent<Unit>>() {})
		)
				.addInput(reduceNode, true, false)
				.setParallelism(1);

		LabyNode.translateAll(env);

		System.out.println(env.getExecutionPlan());
		env.execute();
	}
}
