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

public final class TupleIntInt implements Serializable {

    public int f0, f1;

    public TupleIntInt() {}

    public TupleIntInt(int f0, int f1) {
        this.f0 = f0;
        this.f1 = f1;
    }



    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TupleIntInt that = (TupleIntInt) o;

        if (f0 != that.f0) return false;
        return f1 == that.f1;

    }

    @Override
    public int hashCode() {
        int result = f0;
        result = 31 * result + f1;
        return result;
    }

    @Override
    public String toString() {
        return "TupleIntInt{" +
                "f0=" + f0 +
                ", f1=" + f1 +
                '}';
    }


    public static TupleIntInt of(int f0, int f1) {
        return new TupleIntInt(f0, f1);
    }


    // ------------------------- Serializers -------------------------

    public static final class TupleIntIntSerializer extends TypeSerializer<TupleIntInt> {

        @Override
        public TypeSerializerConfigSnapshot snapshotConfiguration() {
            return null;
        }

        @Override
        public CompatibilityResult<TupleIntInt> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
            return null;
        }

        @Override
        public boolean isImmutableType() {
            return false;
        }

        @Override
        public TypeSerializer<TupleIntInt> duplicate() {
            return this;
        }

        @Override
        public TupleIntInt createInstance() {
            return new TupleIntInt();
        }

        @Override
        public TupleIntInt copy(TupleIntInt from) {
            return copy(from, new TupleIntInt());
        }

        @Override
        public TupleIntInt copy(TupleIntInt from, TupleIntInt reuse) {
            reuse.f0 = from.f0;
            reuse.f1 = from.f1;
            return reuse;
        }

        @Override
        public int getLength() {
            return 8;
        }

        @Override
        public void serialize(TupleIntInt record, DataOutputView target) throws IOException {
            target.writeInt(record.f0);
            target.writeInt(record.f1);
        }

        @Override
        public TupleIntInt deserialize(DataInputView source) throws IOException {
            return deserialize(createInstance(), source);
        }

        @Override
        public TupleIntInt deserialize(TupleIntInt reuse, DataInputView source) throws IOException {
            reuse.f0 = source.readInt();
            reuse.f1 = source.readInt();
            return reuse;
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            target.write(source, getLength());
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof TupleIntIntSerializer;
        }

        @Override
        public boolean canEqual(Object obj) {
            return obj instanceof TupleIntIntSerializer;
        }

        @Override
        public int hashCode() {
            return 44;
        }
    }
}
