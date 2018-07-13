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

package org.emmalanguage.labyrinth;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.Objects;

public class ElementOrEventTypeInfo<T> extends TypeInformation<ElementOrEvent<T>> {

	private final TypeInformation<T> elemTypeInfo;

	public ElementOrEventTypeInfo(TypeInformation<T> elemTypeInfo) {
		this.elemTypeInfo = elemTypeInfo;
	}

	@Override
	public boolean isBasicType() {
		return false;
	}

	@Override
	public boolean isTupleType() {
		return false;
	}

	@Override
	public int getArity() {
		return 1;
	}

	@Override
	public int getTotalFields() {
		return elemTypeInfo.getTotalFields();
	}

	@Override
	public Class<ElementOrEvent<T>> getTypeClass() {
		return (Class<ElementOrEvent<T>>)(Class<?>)ElementOrEvent.class;
	}

	@Override
	public boolean isKeyType() {
		return false;
	}

	@Override
	public TypeSerializer<ElementOrEvent<T>> createSerializer(ExecutionConfig config) {
		return new ElementOrEvent.ElementOrEventSerializer<>(elemTypeInfo.createSerializer(config));
	}


	@Override
	public String toString() {
		return "ElementOrEventTypeInfo{" +
				"elemTypeInfo=" + elemTypeInfo +
				'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		ElementOrEventTypeInfo<?> that = (ElementOrEventTypeInfo<?>) o;
		return Objects.equals(elemTypeInfo, that.elemTypeInfo);
	}

	@Override
	public int hashCode() {
		return Objects.hash(elemTypeInfo)*2;
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof ElementOrEventTypeInfo;
	}
}
