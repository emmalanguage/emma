package org.emma.data.plan;

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
