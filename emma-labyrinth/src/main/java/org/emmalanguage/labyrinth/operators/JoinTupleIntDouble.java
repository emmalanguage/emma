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

import org.emmalanguage.labyrinth.util.TupleIntDouble;
import org.emmalanguage.labyrinth.util.SerializedBuffer;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
//import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectRBTreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Joins (key,b) (build) with (key,c) (probe), giving (b, key, c) to the udf.
 * The first input is the build side.
 */
public abstract class JoinTupleIntDouble<OUT> extends BagOperator<TupleIntDouble, OUT> implements ReusingBagOperator {

    private static final Logger LOG = LoggerFactory.getLogger(JoinTupleIntDouble.class);

    //private Int2ObjectOpenHashMap<DoubleArrayList> ht;
    private Int2ObjectRBTreeMap<DoubleArrayList> ht;
    private SerializedBuffer<TupleIntDouble> probeBuffered;
    private boolean buildDone;
    private boolean probeDone;

    private boolean reuse = false;

    @Override
    public void openOutBag() {
        super.openOutBag();
        probeBuffered = new SerializedBuffer<>(new TupleIntDouble.TupleIntDoubleSerializer());
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
                //ht = new Int2ObjectOpenHashMap<>(8192);
                ht = new Int2ObjectRBTreeMap<>();
            }
        }
    }

    @Override
    public void pushInElement(TupleIntDouble e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);
        if (logicalInputId == 0) { // build side
            assert !buildDone;
            DoubleArrayList l = ht.get(e.f0);
            if (l == null) {
                l = new DoubleArrayList(2);
                l.add(e.f1);
                ht.put(e.f0,l);
            } else {
                l.add(e.f1);
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
            for (TupleIntDouble e: probeBuffered) {
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

    private void probe(TupleIntDouble p) {
        DoubleArrayList l = ht.get(p.f0);
        if (l != null) {
            for (double b: l) {
                udf(b, p);
            }
        }
    }

    // b is the `value' in the build-side, p is the whole probe-side record (the key is p.f0)
    protected abstract void udf(double b, TupleIntDouble p); // Uses out
}
