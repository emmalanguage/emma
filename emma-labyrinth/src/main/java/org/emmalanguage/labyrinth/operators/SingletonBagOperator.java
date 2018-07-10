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

/**
 * Verifies that input bags contain exactly one element, and closes out bag when the in bag is closed.
 */
public abstract class SingletonBagOperator<IN, OUT> extends BagOperator<IN, OUT> {

	private static final int closedC = -1000000;

	private int c = closedC;

	@Override
	public void openOutBag() {
		super.openOutBag();
		assert c == closedC;
		c = 0;
	}

	@Override
	public void pushInElement(IN e, int logicalInputId) {
		super.pushInElement(e, logicalInputId);
		c++;
	}

	@Override
	public void closeInBag(int inputId) {
		super.closeInBag(inputId);
		assert c == 1; // Each of our input bags should contain exactly one element.  (This can go wrong, for example, when the para is not 1)
		c = closedC;
		out.closeBag();
	}
}
