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

import java.util.HashMap;

public class GroupBy0Min1 extends BagOperator<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

    private static final Logger LOG = LoggerFactory.getLogger(GroupBy0Min1.class);

    private HashMap<Integer, Integer> hm;

    @Override
    public void openOutBag() {
        super.openOutBag();
        hm = new HashMap<>();
    }

    @Override
    public void pushInElement(Tuple2<Integer, Integer> e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);
        Integer g = hm.get(e.f0);
        if (g == null) {
            hm.put(e.f0, e.f1);
        } else {
            if (e.f1 < g) {
                hm.replace(e.f0, e.f1);
            }
        }
    }

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);
        for (HashMap.Entry e: hm.entrySet()) {
            out.collectElement(Tuple2.of((Integer)e.getKey(), (Integer)e.getValue()));
        }
        hm = null;
        out.closeBag();
    }
}
