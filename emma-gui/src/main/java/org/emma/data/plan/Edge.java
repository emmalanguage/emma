package org.emma.data.plan;

/**
 * Created by Andi on 22.09.2015.
 */
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
