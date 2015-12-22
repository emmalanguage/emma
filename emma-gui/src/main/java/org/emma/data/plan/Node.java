package org.emma.data.plan;

/**
 * Created by Andi on 22.09.2015.
 */
public class Node extends GraphElement {
    public Integer id;
    public String fillColor;
    public NodeType type;
    public String tooltip = "";

    public String getFillColor() {
        return fillColor;
    }

    public Node setFillColor(String fillColor) {
        this.fillColor = fillColor;
        return this;
    }

    public NodeType getType() {
        return type;
    }

    public Node setType(NodeType type) {
        this.type = type;
        return this;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getTooltip() {
        return tooltip;
    }

    public void setTooltip(String tooltip) {
        this.tooltip = tooltip;
    }

    @Override
    public String toString() {
        return "Node{" +
                "id=" + id +
                ", label=" + label +
                '}';
    }
}
