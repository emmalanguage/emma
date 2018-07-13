package nolaby;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;


/**
 * args: path, numDays
 *
 * The input is at path/input in tab-separated edge-list files named 1, 2, ..., numDays
 */

public class PageRankDiffs {

	private static final Logger LOG = LoggerFactory.getLogger(PageRankDiffs.class);

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		//env.getConfig().setParallelism(1);

		env.getConfig().enableObjectReuse();

		final double d = 0.85;
		final double epsilon = 0.0000000001;

		String pref = args[0] + "/";
		final String yesterdayPRTmpFilename = pref + "tmp/yesterdayCounts";
		final String todayPRTmpFilename = pref + "tmp/todayCounts";
		FileSystem fs = FileSystem.get(new URI(pref));

		final int days = Integer.parseInt(args[1]);
		for (int day = 1; day <= days; day++) {

			LOG.info("### Day " + day);

			DataSet<Tuple2<IntValue, IntValue>> edges = env.readCsvFile(pref + "/input/" + day)
					.fieldDelimiter("\t")
					.lineDelimiter("\n")
					.types(IntValue.class, IntValue.class);

			// (from, to, degree) triples
			DataSet<Tuple3<IntValue, IntValue, IntValue>> edgesWithDeg =
					edges.map(new MapFunction<Tuple2<IntValue, IntValue>, Tuple2<IntValue, IntValue>>() {

						Tuple2<IntValue, IntValue> reuse = Tuple2.of(new IntValue(-1), new IntValue(-2));

						@Override
						public Tuple2<IntValue, IntValue> map(Tuple2<IntValue, IntValue> value) throws Exception {
							reuse.f0.setValue(value.f0);
							reuse.f1.setValue(1);
							return reuse;
						}
					}).groupBy(0).sum(1)
					.join(edges).where(0).equalTo(0)
							.with(new JoinFunction<Tuple2<IntValue, IntValue>, Tuple2<IntValue, IntValue>, Tuple3<IntValue, IntValue, IntValue>>() {
						Tuple3<IntValue, IntValue, IntValue> reuse = Tuple3.of(new IntValue(-1), new IntValue(-2), new IntValue(-3));
						@Override
						public Tuple3<IntValue, IntValue, IntValue> join(Tuple2<IntValue, IntValue> first, Tuple2<IntValue, IntValue> second) throws Exception {
							reuse.f0.setValue(second.f0);
							reuse.f1.setValue(second.f1);
							reuse.f2.setValue(first.f1);
							return reuse;
						}
					});

			DataSet<IntValue> pages = edges.flatMap(new FlatMapFunction<Tuple2<IntValue, IntValue>, IntValue>() {
				@Override
				public void flatMap(Tuple2<IntValue, IntValue> value, Collector<IntValue> out) throws Exception {
					out.collect(value.f0);
					out.collect(value.f1);
				}
			}).distinct();

			DataSet<Double> initWeight = pages.map(new MapFunction<IntValue, Tuple1<IntValue>>() {
				Tuple1<IntValue> reuse = Tuple1.of(new IntValue(1));
				@Override
				public Tuple1<IntValue> map(IntValue value) throws Exception {
					return reuse;
				}
			}).sum(0)
			.map(new MapFunction<Tuple1<IntValue>, Double>() {
				@Override
				public Double map(Tuple1<IntValue> value) throws Exception {
					return 1.0d / value.f0.getValue();
				}
			});

			DataSet<Tuple2<IntValue, DoubleValue>> initPR = pages.map(new RichMapFunction<IntValue, Tuple2<IntValue, DoubleValue>>() {

				private double initWeight = -2;

				private Tuple2<IntValue, DoubleValue> reuse = Tuple2.of(new IntValue(), new DoubleValue());

				@Override
				public void open(Configuration parameters) throws Exception {
					super.open(parameters);
					List<Double> bv = getRuntimeContext().getBroadcastVariable("initWeight");
					assert bv.size() == 1;
					initWeight = bv.get(0);
				}

				@Override
				public Tuple2<IntValue, DoubleValue> map(IntValue id) throws Exception {
					reuse.f0.setValue(id);
					reuse.f1.setValue(initWeight);
					return reuse;
				}

			}).withBroadcastSet(initWeight, "initWeight");

			// --- PageRank Iteration ---

			IterativeDataSet<Tuple2<IntValue, DoubleValue>> PR = initPR.iterate(1000000000);

			// (from, to, degree, rank) => (to, rank / degree)
			DataSet<Tuple2<IntValue, DoubleValue>> msgs = PR.join(edgesWithDeg).where(0).equalTo(0)
					.with(new JoinFunction<Tuple2<IntValue,DoubleValue>, Tuple3<IntValue,IntValue,IntValue>, Tuple2<IntValue, DoubleValue>>() {
				private Tuple2<IntValue, DoubleValue> reuse = Tuple2.of(new IntValue(), new DoubleValue());
				@Override
				public Tuple2<IntValue, DoubleValue> join(Tuple2<IntValue, DoubleValue> first, Tuple3<IntValue, IntValue, IntValue> second) throws Exception {
					// first:  (id, rank)
					// second: (from, to, degree)
					reuse.f0.setValue(second.f1);
					reuse.f1.setValue(first.f1.getValue() / second.f2.getValue());
					return reuse;
				}
			}).groupBy(0).sum(1);

			DataSet<Tuple2<IntValue, DoubleValue>> newPR = msgs.rightOuterJoin(PR).where(0).equalTo(0)
					.with(new RichJoinFunction<Tuple2<IntValue, DoubleValue>, Tuple2<IntValue, DoubleValue>, Tuple2<IntValue, DoubleValue>>() {

				private double jump = -1;

				private Tuple2<IntValue, DoubleValue> reuse = Tuple2.of(new IntValue(), new DoubleValue());

				@Override
				public void open(Configuration parameters) throws Exception {
					super.open(parameters);
					List<Double> bv = getRuntimeContext().getBroadcastVariable("initWeight");
					assert bv.size() == 1;
					jump  = (1-d) * bv.get(0);
				}

				@Override
				public Tuple2<IntValue, DoubleValue> join(Tuple2<IntValue, DoubleValue> first, Tuple2<IntValue, DoubleValue> second) throws Exception {
					// first:  (id, newRank)
					// second: (id, oldRank)
					if (first == null) {
						return second;
					} else {
						// d * newrank + (1-d) * initWeight)
						reuse.f0.setValue(first.f0);
						reuse.f1.setValue(d * first.f1.getValue() + jump);
						return reuse;
					}
				}
			}).withBroadcastSet(initWeight, "initWeight");

			DataSet<Tuple1<DoubleValue>> termCrit = newPR.join(PR).where(0).equalTo(0)
					.with(new JoinFunction<Tuple2<IntValue, DoubleValue>, Tuple2<IntValue, DoubleValue>, Tuple1<DoubleValue>>() {
						private Tuple1<DoubleValue> reuse = Tuple1.of(new DoubleValue());
						@Override
						public Tuple1<DoubleValue> join(Tuple2<IntValue, DoubleValue> first, Tuple2<IntValue, DoubleValue> second) throws Exception {
							reuse.f0.setValue(Math.abs(first.f1.getValue() - second.f1.getValue()));
							return reuse;
						}
					}).sum(0)
					.flatMap(new FlatMapFunction<Tuple1<DoubleValue>, Tuple1<DoubleValue>>() {
						@Override
						public void flatMap(Tuple1<DoubleValue> value, Collector<Tuple1<DoubleValue>> out) throws Exception {
							System.out.println("=== Delta in PageRank: " + value.f0.getValue());
							if (value.f0.getValue() > epsilon) {
								out.collect(value);
							}
						}
					});

			DataSet<Tuple2<IntValue, DoubleValue>> finalPR = PR.closeWith(newPR, termCrit);

			// --- End of PageRank Iteration ---

			//finalPR.writeAsText(pref + "allPRs/noCFL/" + day, FileSystem.WriteMode.OVERWRITE); //.setParallelism(1); // debugging

			if (day != 1) {

				DataSet<Tuple2<IntValue, DoubleValue>> yesterdayPR = env.readCsvFile(yesterdayPRTmpFilename).types(IntValue.class, DoubleValue.class);

				DataSet<Tuple1<DoubleValue>> diffs = finalPR.fullOuterJoin(yesterdayPR).where(0).equalTo(0).with(new JoinFunction<Tuple2<IntValue,DoubleValue>, Tuple2<IntValue,DoubleValue>, Tuple1<DoubleValue>>() {

					Tuple2<IntValue, DoubleValue> nulla = Tuple2.of(new IntValue(), new DoubleValue());

					Tuple1<DoubleValue> reuse = Tuple1.of(new DoubleValue());

					@Override
					public Tuple1<DoubleValue> join(Tuple2<IntValue, DoubleValue> first, Tuple2<IntValue, DoubleValue> second) throws Exception {
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

				diffs.sum(0).map(new MapFunction<Tuple1<DoubleValue>, String>() {
					@Override
					public String map(Tuple1<DoubleValue> dv) throws Exception {
						return dv.f0.toString();
					}
				}).setParallelism(1).writeAsText(pref + "out/expected/diff_" + day, FileSystem.WriteMode.OVERWRITE);
			}

			// Workaround for https://issues.apache.org/jira/browse/FLINK-1268
			fs.delete(new Path(todayPRTmpFilename), true);

			finalPR.writeAsCsv(todayPRTmpFilename, FileSystem.WriteMode.OVERWRITE);

			//System.out.println(env.getExecutionPlan());
			env.execute();

			// yesterdayPRTmpFilename and todayPRTmpFilename has to be different, because the reading and writing can overlap
			fs.delete(new Path(yesterdayPRTmpFilename), true);
			fs.rename(new Path(todayPRTmpFilename), new Path(yesterdayPRTmpFilename));
		}
	}
}
