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
import org.apache.flink.api.common.typeutils.TypeSerializer;

abstract public class OpWithSingletonSide<IN, OUT> extends OpWithSideInput<IN, OUT> {

    private IN sideSing;

    public OpWithSingletonSide(TypeSerializer<IN> inSer) {
        super(inSer);
    }

    @Override
    public void openOutBag() {
        super.openOutBag();
        sideSing = null;
    }

    @Override
    public void closeInBag(int inputId) {
        if (inputId == 0) {
            // side
            for (IN e: sideBuffered) {
                assert sideSing == null;
                sideSing = e;
            }
        }
        super.closeInBag(inputId);
    }

    @Override
    protected void pushInElementWithSide(IN e, SerializedBuffer<IN> side) {
        assert this.sideSing != null;
        pushInElementWithSingletonSide(e, this.sideSing);
    }

    abstract protected void pushInElementWithSingletonSide(IN e, IN side);
}
