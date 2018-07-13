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

import org.emmalanguage.labyrinth.util.SerializedBuffer;

import scala.util.Either;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;



public abstract class JoinScala<A, B, K> extends BagOperator<Either<A, B>, Tuple2<A, B>> implements ReusingBagOperator {

	protected abstract K keyExtr1(A e);
	protected abstract K keyExtr2(B e);

	private HashMap<K, ArrayList<Either<A, B>>> ht;
	private SerializedBuffer<Either<A, B>> probeBuffered;
	private boolean buildDone;
	private boolean probeDone;

	private boolean reuse = false;

	@Override
	public void openOutBag() {
		super.openOutBag();
		probeBuffered = new SerializedBuffer<>(inSer);
		buildDone = false;
		probeDone = false;
		reuse = false;
	}

	@Override
	public void signalReuse() {
		reuse = true;
	}

	@Override
	public void openInBag(int logicalInputId) {
		super.openInBag(logicalInputId);

		if (logicalInputId == 0) {
			// build side
			if (!reuse) {
				ht = new HashMap<>(8192);
			}
		}
	}

	@Override
	public void pushInElement(Either<A, B> e, int logicalInputId) {
		super.pushInElement(e, logicalInputId);
		if (logicalInputId == 0) { // build side
			assert !buildDone;
			K key = keyExtr(e);
			ArrayList<Either<A, B>> l = ht.get(key);
			if (l == null) {
				l = new ArrayList<>();
				l.add(e);
				ht.put(key,l);
			} else {
				l.add(e);
			}
		} else { // probe side
			if (!buildDone) {
				probeBuffered.add(e);
			} else {
				probe(e);
			}
		}
	}

	@Override
	public void closeInBag(int inputId) {
		super.closeInBag(inputId);
		if (inputId == 0) { // build side
			assert !buildDone;
			//LOG.info("Build side finished");
			buildDone = true;
			for (Either<A, B> e: probeBuffered) {
				probe(e);
			}
			if (probeDone) {
				out.closeBag();
			}
		} else { // probe side
			assert inputId == 1;
			assert !probeDone;
			//LOG.info("Probe side finished");
			probeDone = true;
			if (buildDone) {
				out.closeBag();
			}
		}
	}

	private void probe(Either<A, B> e) {
		ArrayList<Either<A, B>> l = ht.get(keyExtr(e));
		if (l != null) {
			for (Either<A, B> a: l) {
				out.collectElement(Tuple2.apply(a.left().get(), e.right().get()));
			}
		}
	}

	protected K keyExtr(Either<A, B> e) {
		if (e.isLeft()) {
			return keyExtr1(e.left().get());
		} else {
			return keyExtr2(e.right().get());
		}
	}
}
