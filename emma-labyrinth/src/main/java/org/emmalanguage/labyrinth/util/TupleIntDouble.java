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

import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.io.Serializable;

public final class TupleIntDouble implements Serializable {

    public int f0;
    public double f1;

    public TupleIntDouble() {}

    public TupleIntDouble(int f0, double f1) {
        this.f0 = f0;
        this.f1 = f1;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TupleIntDouble that = (TupleIntDouble) o;

        if (f0 != that.f0) return false;
        return Double.compare(that.f1, f1) == 0;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = f0;
        temp = Double.doubleToLongBits(f1);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

//    @Override
//    public String toString() {
//        return "TupleIntDouble{" +
//                "f0=" + f0 +
//                ", f1=" + f1 +
//                '}';
//    }

    @Override
    public String toString() {
        return "(" + f0 + ',' + f1 + ')';
    }

    public static TupleIntDouble of(int f0, double f1) {
        return new TupleIntDouble(f0, f1);
    }


    // ------------------------- Serializers -------------------------

    public static final class TupleIntDoubleSerializer extends TypeSerializer<TupleIntDouble> {

        @Override
        public TypeSerializerConfigSnapshot snapshotConfiguration() {
            return null;
        }

        @Override
        public CompatibilityResult<TupleIntDouble> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
            return null;
        }

        @Override
        public boolean isImmutableType() {
            return false;
        }

        @Override
        public TypeSerializer<TupleIntDouble> duplicate() {
            return this;
        }

        @Override
        public TupleIntDouble createInstance() {
            return new TupleIntDouble();
        }

        @Override
        public TupleIntDouble copy(TupleIntDouble from) {
            return copy(from, new TupleIntDouble());
        }

        @Override
        public TupleIntDouble copy(TupleIntDouble from, TupleIntDouble reuse) {
            reuse.f0 = from.f0;
            reuse.f1 = from.f1;
            return reuse;
        }

        @Override
        public int getLength() {
            return 12;
        }

        @Override
        public void serialize(TupleIntDouble record, DataOutputView target) throws IOException {
            target.writeInt(record.f0);
            target.writeDouble(record.f1);
        }

        @Override
        public TupleIntDouble deserialize(DataInputView source) throws IOException {
            return deserialize(createInstance(), source);
        }

        @Override
        public TupleIntDouble deserialize(TupleIntDouble reuse, DataInputView source) throws IOException {
            reuse.f0 = source.readInt();
            reuse.f1 = source.readDouble();
            return reuse;
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            target.write(source, getLength());
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof TupleIntDoubleSerializer;
        }

        @Override
        public boolean canEqual(Object obj) {
            return obj instanceof TupleIntDoubleSerializer;
        }

        @Override
        public int hashCode() {
            return 45;
        }
    }
}
