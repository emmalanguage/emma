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

public class UpdateJoin extends BagOperator<Tuple2<Integer,Integer>, Tuple2<Integer,Integer>> {

    private static final Logger LOG = LoggerFactory.getLogger(UpdateJoin.class);

    private HashMap<Integer, Integer> ht;
    private ArrayList<Tuple2<Integer, Integer>> probeBuffered;
    private boolean buildDone;
    private boolean probeDone;

    private int buildCnt;
    private int probeCnt;

    @Override
    public void openOutBag() {
        super.openOutBag();
        ht = new HashMap<>();
        probeBuffered = new ArrayList<>();
        buildDone = false;
        probeDone = false;
        buildCnt = 0;
        probeCnt = 0;
    }

    @Override
    public void pushInElement(Tuple2<Integer, Integer> e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);
        if (logicalInputId == 0) { // build side
            buildCnt++;
            assert !buildDone;
            assert ht.get(e.f0) == null; // should be a primary key
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
            for (Tuple2<Integer, Integer> e: probeBuffered) {
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

    private void probe(Tuple2<Integer, Integer> e) {
        Integer r = ht.replace(e.f0, e.f1);
        assert r != null; // Let's not allow for insertions for the moment.
    }

    private void emitAndClose() {
        for (HashMap.Entry e: ht.entrySet()) {
            out.collectElement(Tuple2.of((Integer)e.getKey(), (Integer)e.getValue()));
        }
        LOG.info("buildCnt: " + buildCnt + ", probeCnt: " + probeCnt);
        out.closeBag();
    }
}
