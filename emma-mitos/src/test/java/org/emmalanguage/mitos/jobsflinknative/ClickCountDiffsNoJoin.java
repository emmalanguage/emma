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

package org.emmalanguage.mitos.jobsflinknative;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.io.TupleCsvInputFormat;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.*;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.URI;


public class ClickCountDiffsNoJoin {

	private static final Logger LOG = LoggerFactory.getLogger(ClickCountDiffsNoJoin.class);

	// /home/ggevay/Dropbox/cfl_testdata/ClickCount 4
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		//env.getConfig().setParallelism(1);

		env.getConfig().enableObjectReuse();

		final String pref = args[0] + "/";

		final int days = Integer.parseInt(args[1]); // 365

		// Dummy yesterdayCounts for the first iteration, because we can't do the if
		DataSet<Tuple2<IntValue, IntValue>> initYesterdayCounts = env.fromElements(Tuple2.of(new IntValue(-1), new IntValue(0)));

		IterativeDataSet<Tuple2<IntValue, IntValue>> yesterdayCounts = initYesterdayCounts.iterate(days);

		final TupleTypeInfo<Tuple1<Integer>> typeInfo = new TupleTypeInfo<>(TypeInformation.of(Integer.class));
		DataSet<Tuple1<Integer>> visits = yesterdayCounts.readFile(new MapFunction<Integer, InputFormat<Tuple1<Integer>, FileInputSplit>>() {
			@Override
			public InputFormat<Tuple1<Integer>, FileInputSplit> map(Integer day) {
				return new TupleCsvInputFormat<>(new Path(pref + "in/clickLog_" + day), "\n", "\t", typeInfo);
			}
		}, typeInfo);

		DataSet<Tuple2<IntValue, IntValue>> counts = visits.map(new MapFunction<Tuple1<Integer>, Tuple2<IntValue, IntValue>>() {
			private final Tuple2<IntValue, IntValue> reuse = Tuple2.of(new IntValue(-1),new IntValue(1));
			@Override
			public Tuple2<IntValue, IntValue> map(Tuple1<Integer> value) throws Exception {
				reuse.f0.setValue(value.f0);
				return reuse;
			}
		}).groupBy(0).sum(1);

		DataSet<Tuple1<IntValue>> diffs = counts.fullOuterJoin(yesterdayCounts).where(0).equalTo(0).with(new JoinFunction<Tuple2<IntValue,IntValue>, Tuple2<IntValue,IntValue>, Tuple1<IntValue>>() {
			private final Tuple2<IntValue, IntValue> nulla = Tuple2.of(new IntValue(0),new IntValue(0));
			private final Tuple1<IntValue> reuse = Tuple1.of(new IntValue(-1));
			@Override
			public Tuple1<IntValue> join(Tuple2<IntValue, IntValue> first, Tuple2<IntValue, IntValue> second) throws Exception {
				if (first == null) {
					first = nulla;
				}
				if (second == null) {
					second = nulla;
				}
				reuse.f0.setValue(Math.abs(first.f1.getValue() - second.f1.getValue()));
				return reuse;
			}
		});

		DataSet<Object> dummy = diffs
				.sum(0).map(new MapFunction<Tuple1<IntValue>, String>() {
					@Override
					public String map(Tuple1<IntValue> integerTuple1) throws Exception {
						return integerTuple1.f0.toString();
					}
				})
				.cross(yesterdayCounts.getSuperstepNumberAsDataSet())
				.flatMap(new FlatMapFunction<Tuple2<String, Integer>, Object>() {
					@Override
					public void flatMap(Tuple2<String, Integer> tup, Collector<Object> out) throws Exception {
						String toWrite = tup.f0;
						int day = tup.f1;
						String path = pref + "out_nojoin/native/diff_" + day;
						LOG.info("Iteration writing file " + path);
						FileSystem fs = FileSystem.get(new URI(path));
						FSDataOutputStream stream = fs.create(new Path(path), FileSystem.WriteMode.OVERWRITE);
						OutputStreamWriter osw = new OutputStreamWriter(stream);
						BufferedWriter bw = new BufferedWriter(osw, 1024*1024);
						bw.write(toWrite);
						bw.close();
						osw.close();
						stream.close();
					}
				});

		DataSet<Tuple2<IntValue, IntValue>> dummy2 = dummy.map(new MapFunction<Object, Tuple2<IntValue, IntValue>>() {
			@Override
			public Tuple2<IntValue, IntValue> map(Object value) throws Exception {
				return null;
			}
		});

		yesterdayCounts.closeWith(counts.union(dummy2)).output(new DiscardingOutputFormat<>());

		//System.out.println(env.getExecutionPlan());
		env.execute();

	}
}
