package org.emma.data.plan;

/**
 * Created by Andi on 22.09.2015.
 */
public enum EdgeType {
    BROADCAST("broadcast"),
    REPARTITION("repartition");

    private final String text;

    EdgeType(final String text) {
        this.text = text;
    }

    public String toString() {
        return text;
    }
}
