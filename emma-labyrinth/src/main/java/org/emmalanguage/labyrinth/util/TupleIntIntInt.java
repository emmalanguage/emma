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

public final class TupleIntIntInt implements Serializable {

    public int f0, f1, f2;

    public TupleIntIntInt() {}

    public TupleIntIntInt(int f0, int f1, int f2) {
        this.f0 = f0;
        this.f1 = f1;
        this.f2 = f2;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TupleIntIntInt that = (TupleIntIntInt) o;

        if (f0 != that.f0) return false;
        if (f1 != that.f1) return false;
        return f2 == that.f2;
    }

    @Override
    public int hashCode() {
        int result = f0;
        result = 31 * result + f1;
        result = 31 * result + f2;
        return result;
    }

    @Override
    public String toString() {
        return "TupleIntIntInt{" +
                "f0=" + f0 +
                ", f1=" + f1 +
                ", f2=" + f2 +
                '}';
    }

    public static TupleIntIntInt of(int f0, int f1, int f2) {
        return new TupleIntIntInt(f0, f1, f2);
    }


    // ------------------------- Serializers -------------------------

    public static final class TupleIntIntIntSerializer extends TypeSerializer<TupleIntIntInt> {

        @Override
        public TypeSerializerConfigSnapshot snapshotConfiguration() {
            return null;
        }

        @Override
        public CompatibilityResult<TupleIntIntInt> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
            return null;
        }

        @Override
        public boolean isImmutableType() {
            return false;
        }

        @Override
        public TypeSerializer<TupleIntIntInt> duplicate() {
            return this;
        }

        @Override
        public TupleIntIntInt createInstance() {
            return new TupleIntIntInt();
        }

        @Override
        public TupleIntIntInt copy(TupleIntIntInt from) {
            return copy(from, new TupleIntIntInt());
        }

        @Override
        public TupleIntIntInt copy(TupleIntIntInt from, TupleIntIntInt reuse) {
            reuse.f0 = from.f0;
            reuse.f1 = from.f1;
            reuse.f2 = from.f2;
            return reuse;
        }

        @Override
        public int getLength() {
            return 12;
        }

        @Override
        public void serialize(TupleIntIntInt record, DataOutputView target) throws IOException {
            target.writeInt(record.f0);
            target.writeInt(record.f1);
            target.writeInt(record.f2);
        }

        @Override
        public TupleIntIntInt deserialize(DataInputView source) throws IOException {
            return deserialize(createInstance(), source);
        }

        @Override
        public TupleIntIntInt deserialize(TupleIntIntInt reuse, DataInputView source) throws IOException {
            reuse.f0 = source.readInt();
            reuse.f1 = source.readInt();
            reuse.f2 = source.readInt();
            return reuse;
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            target.write(source, getLength());
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof TupleIntIntIntSerializer;
        }

        @Override
        public boolean canEqual(Object obj) {
            return obj instanceof TupleIntIntIntSerializer;
        }

        @Override
        public int hashCode() {
            return 45;
        }
    }
}
