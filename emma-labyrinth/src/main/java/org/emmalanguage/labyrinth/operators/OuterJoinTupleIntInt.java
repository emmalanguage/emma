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

import org.emmalanguage.labyrinth.util.TupleIntInt;
import org.emmalanguage.labyrinth.util.SerializedBuffer;
//import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Joins (key,b) (build) with (key,c) (probe), giving (b, key, c) to the udf.
 * The first input is the build side.
 */
public abstract class OuterJoinTupleIntInt<T> extends BagOperator<TupleIntInt, T> implements ReusingBagOperator {

    private static final Logger LOG = LoggerFactory.getLogger(OuterJoinTupleIntInt.class);

    //private Int2ObjectOpenHashMap<HashMapElem> ht;
    private Int2ObjectRBTreeMap<HashMapElem> ht;
    private SerializedBuffer<TupleIntInt> probeBuffered;
    private boolean buildDone;
    private boolean probeDone;

    private boolean reuse = false;

    @Override
    public void openOutBag() {
        super.openOutBag();
        probeBuffered = new SerializedBuffer<>(new TupleIntInt.TupleIntIntSerializer());
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
                //ht = new Int2ObjectOpenHashMap<>(1024, Int2ObjectOpenHashMap.VERY_FAST_LOAD_FACTOR);
                ht = new Int2ObjectRBTreeMap<>();
            }
        }
    }

    @Override
    public void pushInElement(TupleIntInt e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);
        if (logicalInputId == 0) { // build side
            assert !buildDone;
            HashMapElem l = ht.get(e.f0);
            if (l == null) {
                l = new HashMapElem(new IntArrayList(2));
                l.elems.add(e.f1);
                ht.put(e.f0,l);
            } else {
                l.elems.add(e.f1);
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
            for (TupleIntInt e: probeBuffered) {
                probe(e);
            }
            if (probeDone) {
                doRight();
                out.closeBag();
            }
        } else { // probe side
            assert inputId == 1;
            assert !probeDone;
            //LOG.info("Probe side finished");
            probeDone = true;
            if (buildDone) {
                doRight();
                out.closeBag();
            }
        }
    }

    private void probe(TupleIntInt p) {
        HashMapElem l = ht.get(p.f0);
        if (l != null) {
            l.seen = true;
            // found the key in the hashtable
            for (int b: l.elems) {
                inner(b, p);
            }
        } else {
            // not found
            right(p);
        }
    }

    // Iterate on the build side and call left on not seen elements.
    private void doRight() {
        for (HashMapElem e: ht.values()) {
            if (!e.seen) {
                for (int b: e.elems) {
                    left(b);
                }
            }
        }
    }

    // b is the `value' in the build-side, p is the whole probe-side record (the key is p.f0)
    protected abstract void inner(int b, TupleIntInt p); // Uses out

    // p is the whole probe-side record
    protected abstract void right(TupleIntInt p); // Uses out

    // b is the value of one element.
    // (the key is not included, doRight would have to be modified to use int2ObjectEntrySet for that)
    protected abstract void left(int b); // Uses out


    private static final class HashMapElem {

        boolean seen;
        IntArrayList elems;

        HashMapElem(IntArrayList elems) {
            this.seen = false;
            this.elems = elems;
        }
    }
}
