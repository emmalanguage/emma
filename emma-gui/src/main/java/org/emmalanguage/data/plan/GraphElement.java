/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.emmalanguage.data.plan;

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
