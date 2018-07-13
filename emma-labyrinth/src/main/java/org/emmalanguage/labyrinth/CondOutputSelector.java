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

package org.emmalanguage.labyrinth;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;

import java.util.Collections;
import java.util.List;

public class CondOutputSelector<T> implements OutputSelector<ElementOrEvent<T>> {

	private static final int maxSplit = 5;
	private static final List<String>[] cache = new List[maxSplit];

	static {
		for(Integer i=0; i<maxSplit; i++){
			cache[i] = Collections.singletonList(i.toString());
		}
	}

	@Override
	public Iterable<String> select(ElementOrEvent<T> elementOrEvent) {
		return cache[elementOrEvent.splitId];
	}
}
