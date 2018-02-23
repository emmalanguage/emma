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

import org.emmalanguage.labyrinth.BagOperatorHost;
import org.emmalanguage.labyrinth.ElementOrEvent;
import org.emmalanguage.labyrinth.operators.Bagify;
import org.emmalanguage.labyrinth.operators.IdMap;
import org.emmalanguage.labyrinth.partitioners.Always0;
import org.emmalanguage.labyrinth.partitioners.RoundRobin;
import org.emmalanguage.labyrinth.CFLConfig;
import org.emmalanguage.labyrinth.KickoffSource;
import org.emmalanguage.labyrinth.operators.AssertBagEquals;
import org.emmalanguage.labyrinth.partitioners.FlinkPartitioner;
import org.emmalanguage.labyrinth.util.Util;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

import java.util.Arrays;

public class EmptyBags {

	private static TypeSerializer<String> stringSer = TypeInformation.of(String.class).createSerializer(new ExecutionConfig());

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//		Configuration cfg = new Configuration();
//		cfg.setLong("taskmanager.network.numberOfBuffers", 16384);
//		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(100, cfg);

		//env.getConfig().setParallelism(1);

		CFLConfig.getInstance().terminalBBId = 0;
		KickoffSource kickoffSrc = new KickoffSource(0);
		env.addSource(kickoffSrc).addSink(new DiscardingSink<>());
        final int para = env.getParallelism();

		String[] words = new String[]{};


		DataStream<ElementOrEvent<String>> input =
				env.fromCollection(Arrays.asList(words), TypeInformation.of(String.class))
						.transform("bagify", Util.tpe(), new Bagify<>(new RoundRobin<>(para), 0))
                        .setConnectionType(new FlinkPartitioner<>());

		System.out.println(input.getParallelism());

		DataStream<ElementOrEvent<String>> output = input
				.bt("id-map",input.getType(),
				new BagOperatorHost<String, String>(new IdMap<>(), 0, 1, stringSer)
						.addInput(0, 0, true, 0)
						.out(0,0,true, new Always0<>(1)))
				.setConnectionType(new FlinkPartitioner<>());

		output
				.bt("assert", Util.tpe(), new BagOperatorHost<>(new AssertBagEquals<String>(), 0, 2, stringSer)
						.addInput(0, 0, true, 1))
				.setParallelism(1);

		CFLConfig.getInstance().setNumToSubscribe();

		env.execute();
	}
}
