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
import it.unimi.dsi.fastutil.ints.Int2IntMap;
//import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntRBTreeMap;

import java.util.function.Consumer;

public abstract class GroupBy0ReduceTupleIntInt extends BagOperator<TupleIntInt, TupleIntInt> {

    //protected Int2IntOpenHashMap hm;
    protected Int2IntRBTreeMap hm;

    @Override
    public void openOutBag() {
        super.openOutBag();
        //hm = new Int2IntOpenHashMap(1024, Int2IntOpenHashMap.VERY_FAST_LOAD_FACTOR);
        hm = new Int2IntRBTreeMap();
        hm.defaultReturnValue(Integer.MIN_VALUE);
    }

    @Override
    public void pushInElement(TupleIntInt e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);

        int g = hm.putIfAbsent(e.f0, e.f1);
        if (g != hm.defaultReturnValue()) {
            reduceFunc(e, g);
        }
    }

    protected abstract void reduceFunc(TupleIntInt e, int g);

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);

        //hm.int2IntEntrySet().fastForEach(new Consumer<Int2IntMap.Entry>() {
        hm.int2IntEntrySet().forEach(new Consumer<Int2IntMap.Entry>() {
            @Override
            public void accept(Int2IntMap.Entry e) {
                out.collectElement(TupleIntInt.of(e.getIntKey(), e.getIntValue()));
            }
        });

        hm = null;

        out.closeBag();
    }
}
