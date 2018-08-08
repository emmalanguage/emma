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

package org.emmalanguage.labyrinth.jobsnolaby;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.emmalanguage.labyrinth.util.TestFailedException;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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

public class ControlFlowMicrobenchmarkFlinkNative {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		//env.getConfig().setParallelism(1);

		final int numSteps = Integer.parseInt(args[0]);
		final int numElements = Integer.parseInt(args[1]);

		List<Integer> coll = new ArrayList<>(numElements);
		for (int i = 0; i < numElements; i++) {
			coll.add(i);
		}

		DataSet<Integer> in = env.fromCollection(coll).setParallelism(env.getParallelism()).partitionCustom(new Partitioner<Integer>() {
			@Override
			public int partition(Integer x, int numParts) {
				return x % numParts;
			}
		}, new KeySelector<Integer, Integer>() {
			@Override
			public Integer getKey(Integer x) throws Exception {
				return x;
			}
		});

		IterativeDataSet<Integer> it = in.iterate(numSteps);

		DataSet<Integer> inced = it.map(new MapFunction<Integer, Integer>() {
			@Override
			public Integer map(Integer i) throws Exception {
				return i + 1;
			}
		});

		DataSet<Integer> res = it.closeWith(inced);


		res.flatMap(new RichFlatMapFunction<Integer, Object>() {

			int cnt = -10;

			@Override
			public void open(Configuration parameters) throws Exception {
				cnt = 0;
			}

			@Override
			public void close() throws Exception {
				if (cnt != numElements) {
					throw new TestFailedException("Received " + cnt + " number of elements, instead of 1");
				}
			}

			@Override
			public void flatMap(Integer i, Collector<Object> collector) throws Exception {
				cnt++;
			}
		}).setParallelism(1).output(new DiscardingOutputFormat<>());

		//System.out.println(env.getExecutionPlan());
		env.execute();
	}
}
