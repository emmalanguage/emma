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

package org.emmalanguage.labyrinth.jobsold;

import org.emmalanguage.labyrinth.CondOutputSelector;
import org.emmalanguage.labyrinth.operators.IncMap;
import org.emmalanguage.labyrinth.CFLConfig;
import org.emmalanguage.labyrinth.KickoffSource;
import org.emmalanguage.labyrinth.PhiNode;
import org.emmalanguage.labyrinth.operators.AssertEquals;
import org.emmalanguage.labyrinth.operators.Bagify;
import org.emmalanguage.labyrinth.operators.ConditionNode;
import org.emmalanguage.labyrinth.operators.SmallerThan;
import org.emmalanguage.labyrinth.partitioners.RoundRobin;
import org.emmalanguage.labyrinth.util.LogicalInputIdFiller;
import org.emmalanguage.labyrinth.util.Unit;
import org.emmalanguage.labyrinth.util.Util;
import org.emmalanguage.labyrinth.partitioners.Random;
import org.emmalanguage.labyrinth.BagOperatorHost;
import org.emmalanguage.labyrinth.ElementOrEvent;
import org.emmalanguage.labyrinth.partitioners.FlinkPartitioner;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

import java.util.Arrays;

/**
 * // BB 0
 * i = 1
 * do {
 *     // BB 1
 *     i = i + 1
 * } while (i < 100)
 * // BB 2
 * assert i == 100
 */

public class SimpleCF {

	//private static final Logger LOG = LoggerFactory.getLogger(SimpleCF.class);

	private static TypeSerializer<Integer> integerSer = TypeInformation.of(Integer.class).createSerializer(new ExecutionConfig());
	private static TypeSerializer<Boolean> booleanSer = TypeInformation.of(Boolean.class).createSerializer(new ExecutionConfig());

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//		Configuration cfg = new Configuration();
//		cfg.setLong("taskmanager.network.numberOfBuffers", 16384);
//		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(40, cfg);

		//env.getConfig().setParallelism(1);

		final int n = Integer.parseInt(args[0]);

		final int bufferTimeout = 0;

		//env.setBufferTimeout(bufferTimeout); // TODO: ujrafuttatni igy a clusteren, mert lehet, hogy gyorsabb lett. (Bar lokalisan nem merheto kulonbseg.)

		CFLConfig.getInstance().terminalBBId = 2;
		KickoffSource kickoffSrc = new KickoffSource(0,1);
		//KickoffSource kickoffSrc = new KickoffSource(0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2);
		env.addSource(kickoffSrc).addSink(new DiscardingSink<>());


		Integer[] input = new Integer[]{1};

		DataStream<ElementOrEvent<Integer>> inputBag0 =
				env.fromCollection(Arrays.asList(input))
						.transform("bagify",
								Util.tpe(), new Bagify<>(new RoundRobin<>(env.getParallelism()), 0))
						.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Integer>>(){}))
						.setConnectionType(new FlinkPartitioner<>());

		DataStream<ElementOrEvent<Integer>> inputBag = inputBag0.map(new LogicalInputIdFiller<>(0));

		IterativeStream<ElementOrEvent<Integer>> it = inputBag.iterate(1000000000);

		DataStream<ElementOrEvent<Integer>> phi = it
				//.setConnectionType(new Random<>())
				.bt("phi",inputBag.getType(),
						new PhiNode<Integer>(1, 1, integerSer)
								.addInput(0, 0, false, 0)
								.addInput(1, 1, false, 2)
								.out(0, 1, true, new Random<>(env.getParallelism())))
				.setConnectionType(new FlinkPartitioner<>());


		SplitStream<ElementOrEvent<Integer>> incedSplit = phi
				//.setConnectionType(new Random<>())
				.bt("inc-map",inputBag.getType(),
						new BagOperatorHost<>(
								new IncMap(), 1, 2, integerSer)
								.addInput(0, 1, true, 1)
								.out(0,1,false, new Random<>(env.getParallelism())) // back edge
								.out(1,2,false, new Random<>(1)) // out of the loop
								.out(2,1,true, new Random<>(1))) // to exit condition
				.setConnectionType(new FlinkPartitioner<>())
				.split(new CondOutputSelector<>());

		DataStream<ElementOrEvent<Integer>> incedSplitL = incedSplit.select("0").map(new LogicalInputIdFiller<>(1));

		it.closeWith(incedSplitL);

		DataStream<ElementOrEvent<Boolean>> smallerThan = incedSplit.select("2")
				//.setConnectionType(new Random<>())
				.bt("smaller-than",Util.tpe(),
						new BagOperatorHost<>(
								new SmallerThan(n), 1, 3, integerSer)
								.addInput(0, 1, true, 2)
								.out(0,1,true, new Random<>(1)))
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Boolean>>(){}))
				.setParallelism(1);
				//.setConnectionType(new gg.partitioners2.FlinkPartitioner<>()); // ez itt azert nem kell, mert 1->1

		DataStream<ElementOrEvent<Unit>> exitCond = smallerThan
				.bt("exit-cond",Util.tpe(),
						new BagOperatorHost<>(
								new ConditionNode(1,2), 1, 4, booleanSer)
								.addInput(0, 1, true, 3)).setParallelism(1);
				//.setConnectionType(new gg.partitioners2.FlinkPartitioner<>()); // ez itt azert nem kell, mert nincs output

		// Edge going out of the loop
		DataStream<ElementOrEvent<Integer>> output = incedSplit.select("1");

		output.bt("Check i == " + n, Util.tpe(),
				new BagOperatorHost<>(
						new AssertEquals<>(n), 2, 5, integerSer)
						.addInput(0, 1, false, 2)).setParallelism(1);
				//.setConnectionType(new gg.partitioners2.FlinkPartitioner<>()); // ez itt azert nem kell, mert nincs output

		output.addSink(new DiscardingSink<>());

		CFLConfig.getInstance().setNumToSubscribe();

		System.out.println(env.getExecutionPlan());
		env.execute();
	}
}
