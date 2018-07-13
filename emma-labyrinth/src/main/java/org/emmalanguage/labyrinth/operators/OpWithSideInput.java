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

/**
 * This binary operator has a process method only for the second input, and the first input is available there from an Iterable.
 * It buffers up elements from the second input while the first input has not fully arrived.
 * The first input is the side input, and the second input is the main input.
 */
abstract public class OpWithSideInput<IN, OUT> extends BagOperator<IN, OUT> {

    protected SerializedBuffer<IN> sideBuffered;
    private SerializedBuffer<IN> mainBuffered;

    private boolean sideDone, mainDone;

    private final TypeSerializer<IN> inSer;


    public OpWithSideInput(TypeSerializer<IN> inSer) {
        this.inSer = inSer;
    }

    @Override
    public void openOutBag() {
        super.openOutBag();
        sideBuffered = new SerializedBuffer<>(inSer);
        mainBuffered = new SerializedBuffer<>(inSer);
        sideDone = false;
        mainDone = false;
    }

    @Override
    public void pushInElement(IN e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);
        if (logicalInputId == 0) {
            // side
            assert !sideDone;
            sideBuffered.add(e);
        } else {
            // main
            if (!sideDone) {
                mainBuffered.add(e);
            } else {
                pushInElementWithSide(e, sideBuffered);
            }
        }
    }

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);
        if (inputId == 0) {
            // side
            assert !sideDone;
            sideDone = true;
            for (IN e: mainBuffered) {
                pushInElementWithSide(e, sideBuffered);
            }
            mainBuffered = null;
            if (mainDone) {
                out.closeBag();
            }
        } else {
            // main
            assert inputId == 1;
            mainDone = true;
            if (!sideDone) {
                // do nothing
            } else {
                out.closeBag();
            }
        }
    }

    abstract protected void pushInElementWithSide(IN e, SerializedBuffer<IN> side);
}
