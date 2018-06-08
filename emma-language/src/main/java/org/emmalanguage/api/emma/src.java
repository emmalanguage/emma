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
package org.emmalanguage.api.emma;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A holder for the name of the field in the enclosing object, where a serialized code of an
 * annotated method can be found.
 * <p>
 * For example
 * <p>
 * {@code
 * object foo {
 *   // ...
 *   {@literal @}src("applyQ$01")
 *   def apply(...) = { ... }
 * }
 * }
 * <p>
 * Implies that `foo` contains a field named `applyQ$01` which returns the code of `apply` as
 * a string.
 * <p>
 * Instances of this annotation are placed by the `emma.lib` macro, please do not use this
 * directly.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface src {
    String value();
}
