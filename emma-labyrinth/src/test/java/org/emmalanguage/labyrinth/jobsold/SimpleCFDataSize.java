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
import org.emmalanguage.labyrinth.BagOperatorHost;
import org.emmalanguage.labyrinth.CFLConfig;
import org.emmalanguage.labyrinth.ElementOrEvent;
import org.emmalanguage.labyrinth.KickoffSource;
import org.emmalanguage.labyrinth.PhiNode;
import org.emmalanguage.labyrinth.operators.AssertBagEquals;
import org.emmalanguage.labyrinth.operators.AssertEquals;
import org.emmalanguage.labyrinth.operators.Bagify;
import org.emmalanguage.labyrinth.operators.ConditionNode;
import org.emmalanguage.labyrinth.operators.SmallerThan;
import org.emmalanguage.labyrinth.partitioners.FlinkPartitioner;
import org.emmalanguage.labyrinth.partitioners.Random;
import org.emmalanguage.labyrinth.partitioners.RoundRobin;
import org.emmalanguage.labyrinth.util.LogicalInputIdFiller;
import org.emmalanguage.labyrinth.util.Unit;
import org.emmalanguage.labyrinth.util.Util;
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
 * i = 0
 * d = s-element bag of 0s
 * do {
 *     // BB 1
 *     i = i + 1
 *     d.map(x => x + 1)
 * } while (i < n)
 * // BB 2
 * assert i == n
 * assert all elements of d are n // and when testing: d has s elements
 */

public class SimpleCFDataSize {

	//private static final Logger LOG = LoggerFactory.getLogger(SimpleCF.class);

	private static TypeSerializer<Integer> integerSer = TypeInformation.of(Integer.class).createSerializer(new ExecutionConfig());
	private static TypeSerializer<Boolean> booleanSer = TypeInformation.of(Boolean.class).createSerializer(new ExecutionConfig());

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//		Configuration cfg = new Configuration();
//		cfg.setLong("taskmanager.network.numberOfBuffers", 32768); //16384
//		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(60, cfg); //40

		//env.getConfig().setParallelism(1);

		final int n = Integer.parseInt(args[0]);
		final int s = Integer.parseInt(args[1]);

		CFLConfig.getInstance().terminalBBId = 2;
		KickoffSource kickoffSrc = new KickoffSource(0,1);
		env.addSource(kickoffSrc).addSink(new DiscardingSink<>());

		Integer[] input_i = new Integer[]{0};

		Integer[] input_d = new Integer[s];
		for (int i = 0; i < s; i++) {
			input_d[i] = 0;
		}

		DataStream<ElementOrEvent<Integer>> inputBag0_i =
				env.fromCollection(Arrays.asList(input_i))
						.transform("bagify_i",
								Util.tpe(), new Bagify<>(new RoundRobin<>(env.getParallelism()), 0))
						.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Integer>>(){}))
						.setConnectionType(new FlinkPartitioner<>());

		DataStream<ElementOrEvent<Integer>> inputBag0_d =
				env.fromCollection(Arrays.asList(input_d))
						.transform("bagify_d",
								Util.tpe(), new Bagify<>(new RoundRobin<>(env.getParallelism()), 6))
						.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Integer>>(){}))
						.setConnectionType(new FlinkPartitioner<>());

		DataStream<ElementOrEvent<Integer>> inputBag_i = inputBag0_i.map(new LogicalInputIdFiller<>(0));

		DataStream<ElementOrEvent<Integer>> inputBag_d = inputBag0_d.map(new LogicalInputIdFiller<>(0));

		IterativeStream<ElementOrEvent<Integer>> it_i = inputBag_i.iterate(1000000000);

		IterativeStream<ElementOrEvent<Integer>> it_d = inputBag_d.iterate(1000000000);

		DataStream<ElementOrEvent<Integer>> phi_i = it_i
				.bt("phi_i",inputBag_i.getType(),
						new PhiNode<Integer>(1, 1, integerSer)
								.addInput(0, 0, false, 0)
								.addInput(1, 1, false, 2)
								.out(0, 1, true, new Random<>(env.getParallelism())))
				.setConnectionType(new FlinkPartitioner<>());

		DataStream<ElementOrEvent<Integer>> phi_d = it_d
				.bt("phi_d",inputBag_d.getType(),
						new PhiNode<Integer>(1, 7, integerSer)
								.addInput(0, 0, false, 6)
								.addInput(1, 1, false, 8)
								.out(0, 1, true, new Random<>(env.getParallelism())))
				.setConnectionType(new FlinkPartitioner<>());


		SplitStream<ElementOrEvent<Integer>> incedSplit_i = phi_i
				.bt("inc-map_i",inputBag_i.getType(),
						new BagOperatorHost<>(
								new IncMap(), 1, 2, integerSer)
								.addInput(0, 1, true, 1)
								.out(0,1,false, new Random<>(env.getParallelism())) // back edge
								.out(1,2,false, new Random<>(1)) // out of the loop
								.out(2,1,true, new Random<>(1))) // to exit condition
				.setConnectionType(new FlinkPartitioner<>())
				.split(new CondOutputSelector<>());

		SplitStream<ElementOrEvent<Integer>> incedSplit_d = phi_d
				.bt("inc-map_d",inputBag_d.getType(),
						new BagOperatorHost<>(
								new IncMap(), 1, 8, integerSer)
								.addInput(0, 1, true, 7)
								.out(0,1,false, new Random<>(env.getParallelism())) // back edge
								.out(1,2,false, new Random<>(1))) // out of the loop
				.setConnectionType(new FlinkPartitioner<>())
				.split(new CondOutputSelector<>());


		DataStream<ElementOrEvent<Integer>> incedSplitL_i = incedSplit_i.select("0").map(new LogicalInputIdFiller<>(1));

		DataStream<ElementOrEvent<Integer>> incedSplitL_d = incedSplit_d.select("0").map(new LogicalInputIdFiller<>(1));

		it_i.closeWith(incedSplitL_i);

		it_d.closeWith(incedSplitL_d);

		DataStream<ElementOrEvent<Boolean>> smallerThan = incedSplit_i.select("2")
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
		DataStream<ElementOrEvent<Integer>> output_i = incedSplit_i.select("1");

		DataStream<ElementOrEvent<Integer>> output_d = incedSplit_d.select("1");

		output_i.bt("Check i == " + n, Util.tpe(),
				new BagOperatorHost<>(
						new AssertEquals<>(n), 2, 5, integerSer)
						.addInput(0, 1, false, 2)).setParallelism(1);
				//.setConnectionType(new gg.partitioners2.FlinkPartitioner<>()); // ez itt azert nem kell, mert nincs output

		Integer[] correctOut = new Integer[s];
		for (int i = 0; i < s; i++) {
			correctOut[i] = n;
		}

		output_d.bt("Check d == " + n, Util.tpe(),
				new BagOperatorHost<>(
						new AssertBagEquals<>(correctOut), 2, 9, integerSer)
						.addInput(0, 1, false, 8)).setParallelism(1);
		//.setConnectionType(new gg.partitioners2.FlinkPartitioner<>()); // ez itt azert nem kell, mert nincs output

		output_i.addSink(new DiscardingSink<>()).setParallelism(1);

		//output_d.addSink(new DiscardingSink<>()).setParallelism(1);

		CFLConfig.getInstance().setNumToSubscribe();

		//System.out.println(env.getExecutionPlan());
		env.execute();
	}
}
