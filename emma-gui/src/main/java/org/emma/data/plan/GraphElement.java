package org.emma.data.plan;

public abstract class GraphElement {
    public String strokeColor;
    public Integer strokeWidth;
    public String textColor;
    public int[] dashArray;
    public String label;
    public StrokeCap strokeCap;
    public StrokeJoin strokeJoin;

    public String getStrokeColor() {
        return strokeColor;
    }

    public GraphElement setStrokeColor(String strokeColor) {
        this.strokeColor = strokeColor;
        return this;
    }

    public Integer getStrokeWidth() {
        return strokeWidth;
    }

    public GraphElement setStrokeWidth(int strokeWidth) {
        this.strokeWidth = strokeWidth;
        return this;
    }

    public String getTextColor() {
        return textColor;
    }

    public GraphElement setTextColor(String textColor) {
        this.textColor = textColor;
        return this;
    }

    public int[] getDashArray() {
        return dashArray;
    }

    public GraphElement setDashArray(int[] dashArray) {
        this.dashArray = dashArray;
        return this;
    }

    public String getLabel() {
        return label;
    }

    public GraphElement setLabel(String label) {
        this.label = label;
        return this;
    }

    public StrokeCap getStrokeCap() {
        return strokeCap;
    }

    public GraphElement setStrokeCap(StrokeCap strokeCap) {
        this.strokeCap = strokeCap;
        return this;
    }

    public StrokeJoin getStrokeJoin() {
        return strokeJoin;
    }

    public GraphElement setStrokeJoin(StrokeJoin strokeJoin) {
        this.strokeJoin = strokeJoin;
        return this;
    }
}
