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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;

import java.util.ArrayList;
import java.util.List;

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
 */

public class ControlFlowMicrobenchmarkNewJobs {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		//env.getConfig().setParallelism(1);

		final int numSteps = Integer.parseInt(args[0]);
		final int numElements = Integer.parseInt(args[1]);

		List<Integer> coll = new ArrayList<>(numElements);
		for (int i = 0; i < numElements; i++) {
			coll.add(i);
		}

		for (int i=0; i < numSteps; i++) {

			DataSet<Integer> ds = env.fromCollection(coll).setParallelism(env.getParallelism()).partitionCustom(new Partitioner<Integer>() {
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

			DataSet<Integer> inced = ds.map(new MapFunction<Integer, Integer>() {
				@Override
				public Integer map(Integer i) throws Exception {
					return i + 1;
				}
			});

			coll = inced.collect();
			assert coll.size() == numElements;
		}
	}
}
