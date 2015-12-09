package org.emma.data.plan;

/**
 * Created by Andi on 22.09.2015.
 */
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
