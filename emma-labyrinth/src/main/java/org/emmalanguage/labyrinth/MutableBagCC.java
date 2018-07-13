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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.emmalanguage.labyrinth.operators.BagOperator;
import org.emmalanguage.labyrinth.util.TupleIntInt;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.function.Consumer;

public class MutableBagCC extends BagOperatorHost<TupleIntInt, TupleIntInt> {

	private static TypeSerializer<TupleIntInt> tupleIntIntSer = new TupleIntInt.TupleIntIntSerializer();

	private static final Logger LOG = LoggerFactory.getLogger(MutableBagCC.class);

	// Inputok:
	//  -1: semmi (ez a toBag-nel van)
	//  0: toMutable
	//  1: join
	//  2: update

	// Outok:
	//  0, 1, 2: a joinbol a harom kimeno
	//  3: toBag

	// These change together with outCFLSizes, and show which input/output to activate.
	private Queue<Integer> whichInput = new ArrayDeque<>();

	public MutableBagCC(int opID) {
		super(1, opID, tupleIntIntSer);
		op = new MutableBagOperator();
	}

	@Override
	protected boolean updateOutCFLSizes(List<Integer> cfl) {
		int addedBB = cfl.get(cfl.size() - 1);
		outCFLSizes.add(cfl.size()); // mert minden BB-ben van egy muvelet
		if (addedBB == 1) { // mert BB 1-ben ket muvelet is van ra
			outCFLSizes.add(cfl.size());
		}
		switch (addedBB) {
			case 0:
				whichInput.add(0);
				break;
			case 1:
				whichInput.add(1);
				whichInput.add(2);
				break;
			case 2:
				whichInput.add(-1);
				break;
			default:
				assert false;
		}
		return true;
	}

	@Override
	protected void outCFLSizesRemove() {
		super.outCFLSizesRemove(); // itt kell ez a super hivas
		whichInput.remove();
	}

	@Override
	protected void chooseLogicalInputs(int outCFLSize) {
		int inpID = whichInput.peek();
		assert ((MutableBagOperator)op).inpID == inpID;
		if (inpID != -1) {
			assert inputs.get(inpID).inputInSameBlock;
			activateLogicalInput(inpID, outCFLSize, outCFLSize);
		}
	}

	@Override
	protected void chooseOuts() {
		for (Out out: outs) {
			out.active = false;
		}

		int inID = whichInput.peek();
		switch (inID) {
			case -1: // toBag
				outs.get(3).active = true;
				break;
			case 0: // toMutable
				break;
			case 1: // Join
				outs.get(0).active = true;
				outs.get(1).active = true;
				outs.get(2).active = true;
				break;
			case 2: // update
				break;
			default:
				assert false;
				break;
		}
	}

	class MutableBagOperator extends BagOperator<TupleIntInt, TupleIntInt> {

		private final Int2IntOpenHashMap hm = new Int2IntOpenHashMap(8192);

		int inpID = -3;

		public MutableBagOperator() {
			hm.defaultReturnValue(Integer.MIN_VALUE);
		}

		@Override
		public void openOutBag() {
			super.openOutBag();

			int inpID = whichInput.peek();
			((MutableBagOperator)op).inpID = inpID;

			switch (inpID) {
				case -1:
//					for (HashMap.Entry<Integer, TupleIntInt> e: hm.entrySet()) {
//						out.collectElement(e.getValue());
//					}
					hm.int2IntEntrySet().fastForEach(new Consumer<Int2IntMap.Entry>() {
						@Override
						public void accept(Int2IntMap.Entry e) {
							out.collectElement(TupleIntInt.of(e.getIntKey(), e.getIntValue()));
						}
					});
					out.closeBag();
					break;
				case 0:
					hm.clear();
					break;
				case 1:
					// nothing to do here
					break;
				case 2:
					// nothing to do here
					break;
			}
		}

		@Override
		public void pushInElement(TupleIntInt e, int logicalInputId) {
			super.pushInElement(e, logicalInputId);
			switch (logicalInputId) {
				case 0: // toMutable
					assert inpID == 0;
					hm.put(e.f0, e.f1);
					break;
				case 1: // join
					{
						assert inpID == 1;

//						TupleIntInt g = hm.get(e.f0);
//						assert g != null; // az altalanos interface-nel nem, de a CC-nel mindig benne kell lennie
//						if (g.f1 > e.f1) {
//							out.collectElement(e);
//						}

						int g = hm.get(e.f0);
						assert g != hm.defaultReturnValue(); // az altalanos interface-nel nem, de a CC-nel mindig benne kell lennie
						if (g > e.f1) {
							out.collectElement(e);
						}

						break;
					}
				case 2: // update
					assert inpID == 2;
					int present = hm.replace(e.f0, e.f1);
					assert present != hm.defaultReturnValue(); // az altalanos interface-nel nem, de a CC-nel mindig benne kell lennie
					break;
				default:
					assert false;
					break;
			}
		}

		@Override
		public void closeInBag(int inputId) {
			super.closeInBag(inputId);
			out.closeBag();
		}
	}
}
