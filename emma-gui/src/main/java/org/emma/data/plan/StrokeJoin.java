package org.emma.data.plan;

/**
 * Created by Andi on 22.09.2015.
 */
public enum StrokeJoin {
    MITER("miter"),
    ROUND("round"),
    BEVEL("bevel");

    private final String text;

    StrokeJoin(final String text) {
        this.text = text;
    }

    public String toString() {
        return text;
    }
}
