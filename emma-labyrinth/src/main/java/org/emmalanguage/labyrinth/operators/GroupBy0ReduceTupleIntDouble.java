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
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
//import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleRBTreeMap;

import java.util.function.Consumer;

public abstract class GroupBy0ReduceTupleIntDouble extends BagOperator<TupleIntDouble, TupleIntDouble> {

    //protected Int2DoubleOpenHashMap hm;
    protected Int2DoubleRBTreeMap hm;

    @Override
    public void openOutBag() {
        super.openOutBag();
        hm = new Int2DoubleRBTreeMap();
        hm.defaultReturnValue(Double.MIN_VALUE);
    }

    @Override
    public void pushInElement(TupleIntDouble e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);

        double g = hm.putIfAbsent(e.f0, e.f1);
        if (g != hm.defaultReturnValue()) {
            reduceFunc(e, g);
        }
    }

    protected abstract void reduceFunc(TupleIntDouble e, double g);

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);

        //hm.int2DoubleEntrySet().fastForEach(new Consumer<Int2DoubleMap.Entry>() {
        hm.int2DoubleEntrySet().forEach(new Consumer<Int2DoubleMap.Entry>() {
            @Override
            public void accept(Int2DoubleMap.Entry e) {
                out.collectElement(TupleIntDouble.of(e.getIntKey(), e.getDoubleValue()));
            }
        });

        hm = null;

        out.closeBag();
    }
}
