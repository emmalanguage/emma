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

package org.emmalanguage.mitos;

public interface BagOperatorOutputCollector<T> {

	void collectElement(T e);

	/**
	 * closes our partition of the bag
	 *
	 * WARNING:
	 *   Do not alter your internal state after calling closeBag, since the runtime might call openOutBag for your next
	 *   bag before closeBag returns.
	 *   For example, do not do any cleanup in your closeInBag method after calling closeBag because you might clean up
	 *   the newly created state for your next bag instead. In other words, calling closeBag should be the very last
	 *   thing you do in your closeInBag method.
 	 */
	void closeBag();

	void appendToCfl(int[] bbId);
}
