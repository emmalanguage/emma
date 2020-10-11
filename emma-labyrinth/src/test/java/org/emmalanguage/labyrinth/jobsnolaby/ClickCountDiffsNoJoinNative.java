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

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.buffer.BufferAllocator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;


public class ClickCountDiffsNoJoinNative {

	private static final Logger LOG = LoggerFactory.getLogger(ClickCountDiffsNoJoinNative.class);

	// /home/ggevay/Dropbox/cfl_testdata/ClickCount 4
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		//env.getConfig().setParallelism(1);

		env.getConfig().enableObjectReuse();

		String pref = args[0] + "/";
		String yesterdayCountsTmpFilename = pref + "tmp/yesterdayCounts";
		String todayCountsTmpFilename = pref + "tmp/todayCounts";
		FileSystem fs = FileSystem.get(new URI(pref));

		final int days = Integer.parseInt(args[1]); // 365

		// Empty yesterdayCounts
		DataSet<Tuple2<IntValue, IntValue>> initYesterdayCounts = env.fromElements(Tuple2.of(new IntValue(-1), new IntValue(0)));

		IterativeDataSet<Tuple2<IntValue, IntValue>> yesterdayCounts = initYesterdayCounts.iterate(days);

		DataSet<String> fileName = yesterdayCounts.getSuperstepNumberAsDataSet().flatMap(new FlatMapFunction<Integer, String>() {
			@Override
			public void flatMap(Integer day, Collector<String> out) throws Exception {
				out.collect(pref + "in/clickLog_" + day);
			}
		});

		DataSet<IntValue> visits = fileName.flatMap(new FlatMapFunction<String, IntValue>() {
			@Override
			public void flatMap(String path, Collector<IntValue> out) throws Exception {
				LOG.info("Reading file " + path);
				FileSystem fs = FileSystem.get(new URI(path));
				FSDataInputStream stream = fs.open(new Path(path));
				BufferedReader br = new BufferedReader(new InputStreamReader(stream), 1024*1024);
				IntValue reuse = new IntValue(-1);
				String line;
				line = br.readLine();
				while (line != null) {
					reuse.setValue(Integer.parseInt(line));
					out.collect(reuse);
					line = br.readLine();
				}
				stream.close();
			}
		});

		DataSet<Tuple2<IntValue, IntValue>> counts = visits.map(new MapFunction<IntValue, Tuple2<IntValue, IntValue>>() {
			Tuple2<IntValue, IntValue> reuse = Tuple2.of(new IntValue(-1),new IntValue(1));
			@Override
			public Tuple2<IntValue, IntValue> map(IntValue value) throws Exception {
				reuse.f0 = value;
				return reuse;
			}
		}).groupBy(0).sum(1);

		DataSet<Tuple1<IntValue>> diffs = counts.fullOuterJoin(yesterdayCounts).where(0).equalTo(0).with(new JoinFunction<Tuple2<IntValue,IntValue>, Tuple2<IntValue,IntValue>, Tuple1<IntValue>>() {
			Tuple2<IntValue, IntValue> nulla = Tuple2.of(new IntValue(0),new IntValue(0));
			Tuple1<IntValue> reuse = Tuple1.of(new IntValue(-1));
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
						LOG.info("Writing file " + path);
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
