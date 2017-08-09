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
package org.emmalanguage
package lib.graphs

import api.emma

case class Edge[V](src: V, dst: V)

case class LEdge[V, L](@emma.pk src: V, @emma.pk dst: V, label: L)

case class LVertex[V, L](@emma.pk id: V, label: L)

case class Triangle[V](x: V, y: V, z: V)

case class Message[K, V](@emma.pk tgt: K, payload: V)
