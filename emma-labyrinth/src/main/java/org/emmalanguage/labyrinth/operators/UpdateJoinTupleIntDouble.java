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

import it.unimi.dsi.fastutil.ints.Int2DoubleRBTreeMap;
import org.emmalanguage.labyrinth.util.SerializedBuffer;
import org.emmalanguage.labyrinth.util.TupleIntDouble;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
//import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * The updates are the second input.
 */
public class UpdateJoinTupleIntDouble extends BagOperator<TupleIntDouble, TupleIntDouble> {

    private static final Logger LOG = LoggerFactory.getLogger(UpdateJoinTupleIntDouble.class);

    private Int2DoubleRBTreeMap ht;
    private SerializedBuffer<TupleIntDouble> probeBuffered;
    private boolean buildDone;
    private boolean probeDone;

    private int buildCnt;
    private int probeCnt;

    @Override
    public void openOutBag() {
        super.openOutBag();
        //ht = new Int2DoubleOpenHashMap(4096);
        ht = new Int2DoubleRBTreeMap();
        ht.defaultReturnValue(Double.MIN_VALUE);
        probeBuffered = new SerializedBuffer<>(new TupleIntDouble.TupleIntDoubleSerializer());
        buildDone = false;
        probeDone = false;
        buildCnt = 0;
        probeCnt = 0;
    }

    @Override
    public void pushInElement(TupleIntDouble e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);
        if (logicalInputId == 0) { // build side
            buildCnt++;
            assert !buildDone;
            assert ht.get(e.f0) == ht.defaultReturnValue(); // should be a primary key
            ht.put(e.f0, e.f1);
        } else { // probe side
            probeCnt++;
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
                emitAndClose();
            }
        } else { // probe side
            assert inputId == 1;
            assert !probeDone;
            //LOG.info("Probe side finished");
            probeDone = true;
            if (buildDone) {
                emitAndClose();
            }
        }
    }

    private void probe(TupleIntDouble e) {
        double r = ht.replace(e.f0, e.f1);
        assert r != ht.defaultReturnValue(); // Let's not allow for insertions for the moment.
    }

    private void emitAndClose() {
        //ht.int2DoubleEntrySet().fastForEach(new Consumer<Int2DoubleMap.Entry>() {
        ht.int2DoubleEntrySet().forEach(new Consumer<Int2DoubleMap.Entry>() {
            @Override
            public void accept(Int2DoubleMap.Entry e) {
                out.collectElement(TupleIntDouble.of(e.getIntKey(), e.getDoubleValue()));
            }
        });
        LOG.info("buildCnt: " + buildCnt + ", probeCnt: " + probeCnt);
        out.closeBag();
    }
}
