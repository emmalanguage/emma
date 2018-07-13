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

import java.util.ArrayList;
import java.util.HashMap;

public abstract class Join<IN, OUT> extends BagOperator<Tuple2<Integer, IN>, OUT> implements ReusingBagOperator {

    private static final Logger LOG = LoggerFactory.getLogger(Join.class);

    private HashMap<Integer, ArrayList<Tuple2<Integer,IN>>> ht;
    private ArrayList<Tuple2<Integer, IN>> probeBuffered;
    private boolean buildDone;
    private boolean probeDone;

    private boolean reuse = false;

    @Override
    public void openOutBag() {
        super.openOutBag();
        probeBuffered = new ArrayList<>();
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
    public void pushInElement(Tuple2<Integer, IN> e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);
        if (logicalInputId == 0) { // build side
            assert !buildDone;
            ArrayList<Tuple2<Integer,IN>> l = ht.get(e.f0);
            if (l == null) {
                l = new ArrayList<>();
                l.add(e);
                ht.put(e.f0,l);
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
            for (Tuple2<Integer, IN> e: probeBuffered) {
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

    private void probe(Tuple2<Integer, IN> e) {
        ArrayList<Tuple2<Integer, IN>> l = ht.get(e.f0);
        if (l != null) {
            for (Tuple2<Integer, IN> b: l) {
                udf(b, e);
            }
        }
    }

    protected abstract void udf(Tuple2<Integer,IN> a, Tuple2<Integer,IN> b); // Uses `out`
}
