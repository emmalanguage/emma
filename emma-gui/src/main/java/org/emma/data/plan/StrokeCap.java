package org.emma.data.plan;

public enum StrokeCap {
    ROUND("round"),
    SQUARE("square"),
    BUTT("butt");

    private final String text;

    StrokeCap(final String text) {
        this.text = text;
    }


    public String toString() {
        return text;
    }
}
