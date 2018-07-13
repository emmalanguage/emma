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

import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class GroupByString0Count1 extends BagOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> {

	private static final Logger LOG = LoggerFactory.getLogger(GroupByString0Count1.class);

	private HashMap<String, Integer> hm;

	@Override
	public void openOutBag() {
		super.openOutBag();
		hm = new HashMap<>();
	}

	@Override
	public void pushInElement(Tuple2<String, Integer> e, int logicalInputId) {
		super.pushInElement(e, logicalInputId);
		Integer cnt = hm.get(e.f0);
		if (cnt == null) {
			hm.put(e.f0, e.f1);
		} else {
			hm.replace(e.f0, e.f1 + cnt);
		}
	}

	@Override
	public void closeInBag(int inputId) {
		super.closeInBag(inputId);
		for (String k : hm.keySet()) {
			out.collectElement(Tuple2.of(k, hm.get(k)));
		}
		out.closeBag();
		hm = null;
	}
}
