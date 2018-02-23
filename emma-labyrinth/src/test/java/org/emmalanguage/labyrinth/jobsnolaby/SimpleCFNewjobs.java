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
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

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

public class SimpleCFNewjobs {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		//env.getConfig().setParallelism(1);

		final int n = Integer.parseInt(args[0]);

		int i = 0;

		for (int cnt=0; cnt<n; cnt++) {

			DataSet<Integer> ds = env.fromElements(i).setParallelism(env.getParallelism());

			DataSet<Integer> inced = ds.map(new MapFunction<Integer, Integer>() {
				@Override
				public Integer map(Integer i) throws Exception {
					return i + 1;
				}
			});

//			inced.output(new DiscardingOutputFormat<>()).setParallelism(1);
//			System.out.println(env.getExecutionPlan());
//			assert false;

			List<Integer> collected = inced.collect();
			assert collected.size() == 1;
			i = collected.get(0);
		}

		assert i == n;
	}
}
