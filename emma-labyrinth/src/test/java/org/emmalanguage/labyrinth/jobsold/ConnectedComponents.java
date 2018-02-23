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
import org.emmalanguage.labyrinth.CondOutputSelector;
import org.emmalanguage.labyrinth.operators.Bagify;
import org.emmalanguage.labyrinth.operators.DistinctInt;
import org.emmalanguage.labyrinth.operators.IdMap;
import org.emmalanguage.labyrinth.partitioners.Always0;
import org.emmalanguage.labyrinth.partitioners.TupleIntIntBy0;
import org.emmalanguage.labyrinth.partitioners.FlinkPartitioner;
import org.emmalanguage.labyrinth.partitioners.Forward;
import org.emmalanguage.labyrinth.util.LogicalInputIdFiller;
import org.emmalanguage.labyrinth.util.TupleIntInt;
import org.emmalanguage.labyrinth.util.Unit;
import org.emmalanguage.labyrinth.util.Util;
import org.emmalanguage.labyrinth.CFLConfig;
import org.emmalanguage.labyrinth.ElementOrEvent;
import org.emmalanguage.labyrinth.KickoffSource;
import org.emmalanguage.labyrinth.PhiNode;
import org.emmalanguage.labyrinth.operators.AssertBagEquals;
import org.emmalanguage.labyrinth.operators.ConditionNode;
import org.emmalanguage.labyrinth.operators.FlatMap;
import org.emmalanguage.labyrinth.operators.GraphSinkTupleIntInt;
import org.emmalanguage.labyrinth.operators.GroupBy0Min1TupleIntInt;
import org.emmalanguage.labyrinth.operators.JoinTupleIntInt;
import org.emmalanguage.labyrinth.operators.NonEmptyCombiner;
import org.emmalanguage.labyrinth.operators.Or;
import org.emmalanguage.labyrinth.operators.UpdateJoinTupleIntInt;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// // BB 0
// edges = // read graph (each edge has to be present twice: (u,v) and (v,u))
// vertices0 = edges.map((x,y) => x)
// vertices = vertices0.distinct
// labels = vertices.map(x => (x,x))
// updates = labels
// do {
//     // BB 1
//     msgs = updates.join(edges).on(0).equalTo(0).with( ((vid, label), (u,v)) => (v,label) )
//     minMsgs = msgs.groupBy(0).min(1)
//     updates = labels.join(minMsgs).with( ((vid, label), (target, msg)) => if (msg < label) out.collect((target, msg)) )
//     labels = labels.update(updates)
// } while (!updates.isEmpty)
// // BB 2
// labels.write
// assertBagEquals(labels_2, ...)


// SSA
// // BB 0
// edges = // read graph (each edge has to be present twice: (u,v) and (v,u))
// vertices0 = edges.map((x,y) => x)
// vertices = vertices0.distinct
// labels_0 = vertices.map(x => (x,x))
// updates_0 = labels_0
// do {
//     // BB 1
//     labels_1 = phi(labels_0, labels_2)
//     updates_1 = phi(updates_0, updates_2)
//     msgs = edges.join(updates_1).on(0).equalTo(0).with( ((u,v), (vid, label)) => (v,label) )
//     minMsgs = msgs.groupBy(0).min(1)
//     updates_2 = labels_1.join(minMsgs).on(0).equalTo(0).with( ((vid, label), (target, msg)) => if (msg < label) out.collect((target, msg)) )
//     labels_2 = labels_1.update(updates_2)
//     nonEmpty = updates_2.nonEmpty
// } while (nonEmpty)
// // BB 2
// labels_2.write
// assertBagEquals(labels_2, ...)


public class ConnectedComponents {

	private static final Logger LOG = LoggerFactory.getLogger(ConnectedComponents.class);

	private static TypeSerializer<Integer> integerSer = TypeInformation.of(Integer.class).createSerializer(new ExecutionConfig());
	private static TypeSerializer<Boolean> booleanSer = TypeInformation.of(Boolean.class).createSerializer(new ExecutionConfig());
	private static TypeSerializer<TupleIntInt> tupleIntIntSer = new TupleIntInt.TupleIntIntSerializer();

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		if (env instanceof LocalStreamEnvironment) {
			// mert kulonben az otthoni gepen 8 szal lenne, amikoris nem eleg a network buffer
			env.getConfig().setParallelism(4);
		}

//		Configuration cfg = new Configuration();
//		cfg.setLong("taskmanager.network.numberOfBuffers", 32768); //16384
//		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(20, cfg); //20 // amugy sokszor 9-nel jon elo hiba, mivel ekkor mar nehol nem kap minden instance

		//env.getConfig().setParallelism(1);

		int para = env.getParallelism();



		PojoTypeInfo.registerCustomSerializer(ElementOrEvent.class, new ElementOrEvent.ElementOrEventSerializerFactory());
		PojoTypeInfo.registerCustomSerializer(TupleIntInt.class, TupleIntInt.TupleIntIntSerializer.class);



		CFLConfig.getInstance().terminalBBId = 2;
		KickoffSource kickoffSrc = new KickoffSource(0,1);
		env.addSource(kickoffSrc).addSink(new DiscardingSink<>());


		String outFile = ParameterTool.fromArgs(args).get("output");


		DataStream<TupleIntInt> edgesStream = Util.getGraphTuple2IntInt(env, args);

		DataStream<ElementOrEvent<TupleIntInt>> edges =
				edgesStream
						.transform("bagify",
								Util.tpe(), new Bagify<>(new TupleIntIntBy0(para), 14))
						.returns(TypeInformation.of(new TypeHint<ElementOrEvent<TupleIntInt>>(){}))
				.setConnectionType(new FlinkPartitioner<>());

		DataStream<ElementOrEvent<Integer>> vertices0 = edges
				.bt("vertices0", Util.tpe(), new BagOperatorHost<>(new FlatMap<TupleIntInt, Integer>(){
			@Override
			public void pushInElement(TupleIntInt e, int logicalInputId) {
				super.pushInElement(e, logicalInputId);
				out.collectElement(e.f0);
			}
		}, 0, 0, tupleIntIntSer)
			.addInput(0,0,true,14)
			.out(0,0,true, new Forward<>(para)))
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Integer>>(){}))
				.setConnectionType(new FlinkPartitioner<>());

		DataStream<ElementOrEvent<Integer>> vertices = vertices0
				.bt("vertices", Util.tpe(),
				new BagOperatorHost<Integer, Integer>(new DistinctInt(), 0, 1, integerSer)
						.addInput(0,0,true, 0)
						.out(0,0,true, new Forward<>(para)))
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Integer>>(){}))
				.setConnectionType(new FlinkPartitioner<>());

		DataStream<ElementOrEvent<TupleIntInt>> labels_0 = vertices
				.bt("labels_0", Util.tpe(), new BagOperatorHost<>(new FlatMap<Integer,TupleIntInt>(){
			@Override
			public void pushInElement(Integer e, int logicalInputId) {
				super.pushInElement(e, logicalInputId);
				out.collectElement(TupleIntInt.of(e,e));
			}
		}, 0, 2, integerSer)
			.addInput(0,0,true,1)
			.out(0,0,true, new Forward<>(para)))
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<TupleIntInt>>(){}))
				.setConnectionType(new FlinkPartitioner<>());

		DataStream<ElementOrEvent<TupleIntInt>> updates_0 = labels_0
				.bt("updates_0", Util.tpe(), new BagOperatorHost<>(new IdMap<TupleIntInt>(), 0, 3, tupleIntIntSer)
				.addInput(0,0,true,2)
				.out(0,0,true, new Forward<>(para)))
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<TupleIntInt>>(){}))
				.setConnectionType(new FlinkPartitioner<>());


		// --- Iteration starts here ---

		IterativeStream<ElementOrEvent<TupleIntInt>> labelsIt = labels_0.map(new LogicalInputIdFiller<>(0)).iterate(1000000000);

		DataStream<ElementOrEvent<TupleIntInt>> labels_1 = labelsIt
				.bt("labels_1", Util.tpe(), new PhiNode<TupleIntInt>(1, 4, tupleIntIntSer)
				.addInput(0,0,false,2)
				.addInput(1,1,false,9)
				.out(0,1,true, new TupleIntIntBy0(para)) // (this goes to two places, but none of them is conditional)
		)
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<TupleIntInt>>(){}))
				.setConnectionType(new FlinkPartitioner<>());

		IterativeStream<ElementOrEvent<TupleIntInt>> updatesIt = updates_0.map(new LogicalInputIdFiller<>(0)).iterate(1000000000);

		DataStream<ElementOrEvent<TupleIntInt>> updates_1 = updatesIt
				.bt("updates_1", Util.tpe(), new PhiNode<TupleIntInt>(1, 5, tupleIntIntSer)
				.addInput(0,0,false,3)
				.addInput(1,1,false,8)
				.out(0,1,true, new TupleIntIntBy0(para))
		)
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<TupleIntInt>>(){}))
				.setConnectionType(new FlinkPartitioner<>());

		//     msgs = edges.join(updates_1).on(0).equalTo(0).with( ((u,v), (vid, label)) => (v,label) )
		DataStream<ElementOrEvent<TupleIntInt>> msgs = edges
				.map(new LogicalInputIdFiller<>(0))
				.union(updates_1.map(new LogicalInputIdFiller<>(1)))
				.setConnectionType(new FlinkPartitioner<>()) // Meg lehetne nezni, hogy enelkul is mukodik-e
				.bt("msgs", Util.tpe(), new BagOperatorHost<>(new JoinTupleIntInt<TupleIntInt>(){
					@Override
					protected void udf(int b, TupleIntInt p) {
						out.collectElement(TupleIntInt.of(b, p.f1));
					}
				}, 1, 6, tupleIntIntSer)
				.addInput(0,0,false,14) // edges
				.addInput(1,1,true,5) // updates_1
				.out(0,1,true, new TupleIntIntBy0(para))
				)
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<TupleIntInt>>(){}))
				.setConnectionType(new FlinkPartitioner<>());

		//todo: combiner
		DataStream<ElementOrEvent<TupleIntInt>> minMsgs = msgs
				.bt("minMsgs", Util.tpe(), new BagOperatorHost<>(new GroupBy0Min1TupleIntInt(), 1, 7, tupleIntIntSer)
						.addInput(0,1,true,6)
						.out(0,1,true, new TupleIntIntBy0(para)))
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<TupleIntInt>>(){}))
				.setConnectionType(new FlinkPartitioner<>());

		//     updates_2 = labels_1.join(minMsgs).on(0).equalTo(0).with( ((vid, label), (target, msg)) => if (msg < label) out.collect((target, msg)) )
		SplitStream<ElementOrEvent<TupleIntInt>> updates_2 = labels_1
				.map(new LogicalInputIdFiller<>(0))
				.union(minMsgs.map(new LogicalInputIdFiller<>(1)))
				.setConnectionType(new FlinkPartitioner<>())
				.bt("updates_2", Util.tpe(), new BagOperatorHost<>(new JoinTupleIntInt<TupleIntInt>(){
					@Override
					protected void udf(int b, TupleIntInt p) {
						if (b > p.f1) {
							out.collectElement(p);
						}
					}
				}, 1, 8, tupleIntIntSer)
								.addInput(0,1,true,4)
								.addInput(1,1,true,7)
								.out(0,1,true, new TupleIntIntBy0(para)) // To labels_2 update
								.out(1,1,true, new Forward<>(para)) // to nonEmpty
						        .out(2,1,false, new Forward<>(para)) // back-edge
				)
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<TupleIntInt>>(){}))
				.setConnectionType(new FlinkPartitioner<>())
				.split(new CondOutputSelector<>());

		//     labels_2 = labels_1.update(updates_2)
		SplitStream<ElementOrEvent<TupleIntInt>> labels_2 = labels_1
				.map(new LogicalInputIdFiller<>(0))
				.union(updates_2.select("0").map(new LogicalInputIdFiller<>(1)))
				.setConnectionType(new FlinkPartitioner<>())
				.bt("labels_2",Util.tpe(),new BagOperatorHost<>(new UpdateJoinTupleIntInt(),1,9, tupleIntIntSer)
								.addInput(0,1,true,4) // labels_1
								.addInput(1,1,true,8) // updates_2
								.out(0,1,false, new Forward<>(para)) // back-edge
								.out(1,2,false, outFile == null ? new Always0<>(1) : new Forward<>(para)) // out of the loop
				)
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<TupleIntInt>>(){}))
				.setConnectionType(new FlinkPartitioner<>())
				.split(new CondOutputSelector<>());

		labelsIt.closeWith(labels_2.select("0").map(new LogicalInputIdFiller<>(1)));
		updatesIt.closeWith(updates_2.select("2").map(new LogicalInputIdFiller<>(1)));

		// exit cond

		DataStream<ElementOrEvent<Boolean>> nonEmpty = updates_2.select("1")
				.bt("nonEmpty",Util.tpe(),
						new BagOperatorHost<>(
								new NonEmptyCombiner<TupleIntInt>(), 1, 10, tupleIntIntSer)
								.addInput(0, 1, true, 8)
								.out(0,1,true, new Always0<>(1)))
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Boolean>>(){}));

		DataStream<ElementOrEvent<Boolean>> nonEmptyOred = nonEmpty
				.bt("Or",Util.tpe(),
						new BagOperatorHost<>(
								new Or(), 1, 16, booleanSer)
								.addInput(0, 1, true, 10)
								.out(0, 1, true, new Always0<>(1)))
				.returns(TypeInformation.of(new TypeHint<ElementOrEvent<Boolean>>(){}))
				.setParallelism(1);

		DataStream<ElementOrEvent<Unit>> exitCond = nonEmptyOred
				.bt("exit-cond",Util.tpe(),
						new BagOperatorHost<>(
								new ConditionNode(1,2), 1, 11, booleanSer)
								.addInput(0, 1, true, 16))
				.setParallelism(1);

		// --- Iteration ends here ---


		if (outFile == null) {
			labels_2.select("1").bt("AssertBagEquals", Util.tpe(), new BagOperatorHost<>(new AssertBagEquals<>(
					TupleIntInt.of(0, 0),
					TupleIntInt.of(1, 0),
					TupleIntInt.of(2, 0),
					TupleIntInt.of(3, 0),
					TupleIntInt.of(4, 0),
					TupleIntInt.of(5, 5),
					TupleIntInt.of(6, 5),
					TupleIntInt.of(7, 5)
			), 2, 13, tupleIntIntSer)
					.addInput(0, 1, false, 9)).setParallelism(1);
		} else {
			labels_2.select("1").bt("GraphSink", Util.tpe(), new BagOperatorHost<>(new GraphSinkTupleIntInt(outFile), 2, 13, tupleIntIntSer)
					.addInput(0,1,false,9));
		}

		CFLConfig.getInstance().setNumToSubscribe();

		//System.out.println(env.getExecutionPlan());
		env.execute();
	}
}
