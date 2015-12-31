package org.emma.data.plan;

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
