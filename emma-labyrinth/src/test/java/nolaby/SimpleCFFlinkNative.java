package nolaby;

import org.emmalanguage.labyrinth.util.TestFailedException;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

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

public class SimpleCFFlinkNative {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		//env.getConfig().setParallelism(1);

		final int n = Integer.parseInt(args[0]);


		Integer[] input = new Integer[]{0};

		DataSet<Integer> in = env.fromCollection(Arrays.asList(input));

		IterativeDataSet<Integer> it = in.iterate(n);

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
				if (cnt != 1) {
					throw new TestFailedException("Received " + cnt + " number of elements, instead of 1");
				}
			}

			@Override
			public void flatMap(Integer i, Collector<Object> collector) throws Exception {
				cnt++;
				if (i != n) {
					throw new TestFailedException("Received record " + i + " instead of " + n);
				}
			}
		}).setParallelism(1).output(new DiscardingOutputFormat<>());

		System.out.println(env.getExecutionPlan());
		env.execute();
	}
}
