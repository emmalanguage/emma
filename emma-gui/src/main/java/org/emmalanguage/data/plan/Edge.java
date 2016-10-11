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
package org.emmalanguage.data.plan;

public class Edge extends GraphElement {
    public int[] connect = new int[2];
    public EdgeType type;

    public Edge(int from, int to) {
        this.connect[0] = from;
        this.connect[1] = to;
    }

    public Edge(int from, int to, EdgeType type) {
        this.connect[0] = from;
        this.connect[1] = to;
        this.type = type;
    }

    public int[] getConnect() {
        return connect;
    }

    public Edge setConnect(int[] connect) {
        this.connect = connect;
        return this;
    }

    public EdgeType getType() {
        return type;
    }

    public Edge setType(EdgeType type) {
        this.type = type;
        return this;
    }

    @Override
    public String toString() {
        return "Edge{" +
                connect[0]+" -> "+connect[1] +
                '}';
    }
}
