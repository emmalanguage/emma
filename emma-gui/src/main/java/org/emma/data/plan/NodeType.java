package org.emma.data.plan;

/**
 * Created by Andi on 23.09.2015.
 */
public enum NodeType {
    INPUT("input"),
    CONSTANT("constant"),
    DEFAULT("default");

    private final String text;

    NodeType(final String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }
}
