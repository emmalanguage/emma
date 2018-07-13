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

package org.emmalanguage.labyrinth.util;

import org.emmalanguage.labyrinth.ElementOrEvent;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TupleCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class Util {

	private static final Logger LOG = LoggerFactory.getLogger(Util.class);


	public static <T> TypeInformation<ElementOrEvent<T>> tpe() {
		return TypeInformation.of((Class<ElementOrEvent<T>>)(Class)ElementOrEvent.class);
	}


	public static DataStream<Tuple2<Integer, Integer>> getGraph(StreamExecutionEnvironment env, String[] args) {
		@SuppressWarnings("unchecked")
		//Tuple2<Integer, Integer>[] edgesNB0 = new Tuple2[]{Tuple2.of(0,1)};
				Tuple2<Integer, Integer>[] edgesNB0 = new Tuple2[]{
				Tuple2.of(0,1),
				Tuple2.of(1,2),
				Tuple2.of(3,4),
				Tuple2.of(4,0),
				Tuple2.of(5,6),
				Tuple2.of(5,7)
		};

		final ParameterTool params = ParameterTool.fromArgs(args);

		DataStream<Tuple2<Integer, Integer>> edgesStream;

		String file = params.get("edges");
		if (file != null) {
			LOG.info("Reading input from file " + file);
			edgesStream = env.createInput(new TupleCsvInputFormat<>(new Path(file),"\n", "\t", new TupleTypeInfo<>(TypeInformation.of(Integer.class), TypeInformation.of(Integer.class))),
					new TupleTypeInfo<Tuple2<Integer, Integer>>(TypeInformation.of(Integer.class), TypeInformation.of(Integer.class)));
		} else {
			LOG.info("No input file given. Using built-in dataset.");
			edgesStream = env.fromCollection(Arrays.asList(edgesNB0));
		}

		return edgesStream.flatMap(new FlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
			@Override
			public void flatMap(Tuple2<Integer, Integer> value, Collector<Tuple2<Integer, Integer>> out) throws Exception {
				out.collect(value);
				out.collect(Tuple2.of(value.f1, value.f0));
			}
		});
	}

	public static DataStream<TupleIntInt> getGraphTuple2IntInt(StreamExecutionEnvironment env, String[] args) {
		@SuppressWarnings("unchecked")
		//Tuple2<Integer, Integer>[] edgesNB0 = new Tuple2[]{Tuple2.of(0,1)};
				TupleIntInt[] edgesNB0 = new TupleIntInt[]{
				TupleIntInt.of(0,1),
				TupleIntInt.of(1,2),
				TupleIntInt.of(3,4),
				TupleIntInt.of(4,0),
				TupleIntInt.of(5,6),
				TupleIntInt.of(5,7)
		};

		final ParameterTool params = ParameterTool.fromArgs(args);

		DataStream<TupleIntInt> edgesStream;

		String file = params.get("edges");
		if (file != null) {
			LOG.info("Reading input from file " + file);
			edgesStream = env.createInput(new TupleCsvInputFormat<>(new Path(file),"\n", "\t", new TupleTypeInfo<>(TypeInformation.of(Integer.class), TypeInformation.of(Integer.class))),
					new TupleTypeInfo<Tuple2<Integer, Integer>>(TypeInformation.of(Integer.class), TypeInformation.of(Integer.class)))
			.map(new MapFunction<Tuple2<Integer, Integer>, TupleIntInt>() {
				@Override
				public TupleIntInt map(Tuple2<Integer, Integer> value) throws Exception {
					return TupleIntInt.of(value.f0, value.f1);
				}
			});
		} else {
			LOG.info("No input file given. Using built-in dataset.");
			edgesStream = env.fromCollection(Arrays.asList(edgesNB0));
		}

		return edgesStream.flatMap(new FlatMapFunction<TupleIntInt, TupleIntInt>() {
			@Override
			public void flatMap(TupleIntInt value, Collector<TupleIntInt> out) throws Exception {
				out.collect(value);
				out.collect(TupleIntInt.of(value.f1, value.f0));
			}
		});
	}
}
