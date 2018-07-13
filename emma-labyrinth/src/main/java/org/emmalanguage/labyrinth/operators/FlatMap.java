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

package org.emmalanguage.labyrinth.operators;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public abstract class FlatMap<IN,OUT> extends BagOperator<IN,OUT> {

	@Override
	public void openOutBag() {
		super.openOutBag();
	}

	@Override
	public void closeInBag(int inputId) {
		super.closeInBag(inputId);
		out.closeBag();
	}

	public static <IN, OUT> FlatMap<IN, OUT> create(FlatMapFunction<IN, OUT> f) {
		return new FlatMap<IN, OUT>() {
			@Override
			public void pushInElement(IN e, int logicalInputId) {
				super.pushInElement(e, logicalInputId);
				try {
					f.flatMap(e, new Collector<OUT>() {
						@Override
						public void collect(OUT x) {
							out.collectElement(x);
						}

						@Override
						public void close() {
							out.closeBag();
						}
					});
				} catch (Exception e1) {
					assert false;
				}
			}
		};
	}
}
