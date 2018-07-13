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

import org.emmalanguage.labyrinth.util.Unit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConditionNode extends SingletonBagOperator<Boolean, Unit> {

	protected static final Logger LOG = LoggerFactory.getLogger(ConditionNode.class);

	private final int[] trueBranchBbIds;
	private final int[] falseBranchBbIds;
	
	public ConditionNode(int trueBranchBbId, int falseBranchBbId) {
		this(new int[]{trueBranchBbId}, new int[]{falseBranchBbId});
	}

	public ConditionNode(int[] trueBranchBbIds, int[] falseBranchBbIds) {
		this.trueBranchBbIds = trueBranchBbIds;
		this.falseBranchBbIds = falseBranchBbIds;
	}

	@Override
	public void pushInElement(Boolean e, int logicalInputId) {
		super.pushInElement(e, logicalInputId);
		for (int b: e ? trueBranchBbIds : falseBranchBbIds) {
			out.appendToCfl(b);
		}
	}
}
