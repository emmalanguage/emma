package org.emma.data.plan;

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
