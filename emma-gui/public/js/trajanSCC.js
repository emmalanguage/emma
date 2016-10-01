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
function TarjanGraph(vertices){
    this.vertices = vertices || [];
}

function Vertex(name){
    this.name = name || null;
    this.connections = [];
    this.index= -1;
    this.lowlink = -1;
}

Vertex.prototype = {
    equals: function(vertex){
        return (vertex.name && this.name==vertex.name);
    }
};

function VertexStack(vertices) {
    this.vertices = vertices || [];
}

VertexStack.prototype = {
    contains: function(vertex){
        for (var i in this.vertices){
            if (this.vertices[i].equals(vertex)){
                return true;
            }
        }
        return false;
    }
};

function Tarjan(graph) {
    this.index = 0;
    this.stack = new VertexStack();
    this.graph = graph;
    this.scc = [];
}

Tarjan.prototype = {
    run: function(){
        for (var i in this.graph.vertices){
            if (this.graph.vertices[i].index<0){
                this.strongconnect(this.graph.vertices[i]);
            }
        }
        return this.scc;
    },
    strongconnect: function(vertex){
        vertex.index = this.index;
        vertex.lowlink = this.index;
        this.index = this.index + 1;
        this.stack.vertices.push(vertex);

        for (var i in vertex.connections){
            var v = vertex;
            var w = vertex.connections[i];
            if (w.index<0){
                this.strongconnect(w);
                v.lowlink = Math.min(v.lowlink,w.lowlink);
            } else if (this.stack.contains(w)){
                v.lowlink = Math.min(v.lowlink,w.index);
            }
        }

        if (vertex.lowlink==vertex.index){
            var vertices = [];
            var w = null;
            if (this.stack.vertices.length>0){
                do {
                    w = this.stack.vertices.pop();
                    vertices.push(w);
                } while (!vertex.equals(w));
            }

            if (vertices.length>1){
                this.scc.push(vertices);
            }
        }
    }
};