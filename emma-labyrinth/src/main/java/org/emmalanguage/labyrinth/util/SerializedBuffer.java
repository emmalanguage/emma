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

package org.emmalanguage.labyrinth.util;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.HeapMemorySegment;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentSource;
import org.apache.flink.runtime.io.disk.RandomAccessInputView;
import org.apache.flink.runtime.io.disk.SimpleCollectingOutputView;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class SerializedBuffer<T> implements Iterable<T> {

    private static final int segSize = 32768;

    private final TypeSerializer<T> ser;

    private final ArrayList<MemorySegment> segs = new ArrayList<>();

    private final SimpleCollectingOutputView outView = new SimpleCollectingOutputView(segs, new ConjuringSegmentSource(), segSize);

    // Note that this doesn't mean what it used to mean (that the iterator was called). Now it is set to true also when
    // the subpartition was not dammed at all. (Since the operator essentially consumed the elements, but not through
    // the iterator but through processElement. (This is needed, because it can happen that the input will be needed
    // later even when it didn't dam.))
    public boolean consumeStarted = false;

    private int numWritten = 0;

    public SerializedBuffer(TypeSerializer<T> ser) {
        this.ser = ser;
    }

    public void add(T e) {
        //assert !consumeStarted; // this doesn't work, because processElement adds more stuff after consume
        numWritten++;
        try {
            ser.serialize(e, outView);
        } catch (IOException e1) {
            throw new RuntimeException(e1);
        }
    }

    public int size() {
        return numWritten;
    }

    @Override
    public Iterator<T> iterator() {
        consumeStarted = true;
        return new ElementIterator();
    }

    public final class ElementIterator implements Iterator<T> {

        RandomAccessInputView inView = new RandomAccessInputView(segs, segSize);

        private int numRead = 0;

        @Override
        public boolean hasNext() {
            assert numRead <= numWritten;
            return numRead < numWritten;
        }

        @Override
        public T next() {
            numRead++;
            assert numRead <= numWritten;
            try {
                return ser.deserialize(inView);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }


    private static final class ConjuringSegmentSource implements MemorySegmentSource {
        @Override
        public MemorySegment nextSegment() {
            return HeapMemorySegment.FACTORY.allocateUnpooledSegment(segSize, null);
        }
    }
}
