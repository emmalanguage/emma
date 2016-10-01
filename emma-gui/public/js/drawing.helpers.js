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
function arrow(obj) {
	var segments = obj.segments,
	    path = new paper.Path();

    if (obj.segments == null || obj.segments.length == 0) {
        console.error("No segments to draw.", obj);
        return;
    }

    path.strokeColor = obj.strokeColor;
    path.dashArray = obj.dashArray || 1;
    path.strokeWidth = obj.strokeWidth || 1;
    path.strokeCap = obj.strokeCap || "round";
    path.strokeJoin = obj.strokeJoin || "round";

    var lastPoint = segments[segments.length-2];
    var firstPoint = segments[1];

	//first point
	if (obj.startArrow == "cone" || obj.startArrow == "circle") {
	    firstPoint = shortVector(segments[1][0], segments[1][1], segments[0][0], segments[0][1]);
	    path.add(new paper.Point(firstPoint[0], firstPoint[1]));
	} else {
	    path.add(new paper.Point(segments[0][0], segments[0][1]));
	}

    //middle points
    for (var i = 1; i < segments.length - 1; i++) {
        path.lineTo(new paper.Point(segments[i][0], segments[i][1]));
    }

    //end point
    if (obj.endArrow == "cone" || obj.endArrow == "circle") {
        lastPoint = shortVector(segments[segments.length-2][0], segments[segments.length-2][1], segments[segments.length-1][0], segments[segments.length-1][1]);
        path.lineTo(new paper.Point(lastPoint[0], lastPoint[1]));
    } else {
        path.lineTo(new paper.Point(segments[segments.length-1][0], segments[segments.length-1][1]));
    }

    //arrows
    if (obj.startArrow != null)
        drawArrow(
            obj.startArrow,
            firstPoint,
            segments[0],
            obj.strokeColor
        );

    if (obj.endArrow != null)
        drawArrow(
            obj.endArrow,
            lastPoint,
            segments[segments.length-1],
            obj.strokeColor
        );

    if (obj.label != null)
        drawLabel(obj);

	return path;
}

function drawArrow(type, p1, p2, color) {
    if (type == "cone") {
        var points = calcCone(p1[0], p1[1], p2[0], p2[1]);

        new paper.Path({
            segments: [
                new paper.Point(p1[0], p1[1]),
                new paper.Point(points[0], points[1]),
                new paper.Point(points[2], points[3]),
                new paper.Point(p1[0], p1[1])
            ],
            strokeColor: color
        });
    } else if (type == "arrow") {
        var endPoints = calcArrow(p1[0], p1[1], p2[0], p2[1]);

        var e0 = endPoints[0],
            e1 = endPoints[1],
            e2 = endPoints[2],
            e3 = endPoints[3];

        new paper.Path({
            segments: [
                new paper.Point(e0, e1),
                new paper.Point(p2[0], p2[1]),
                new paper.Point(e2, e3)
            ],
            strokeColor: color,
            fillColor: color
        });
    } else if (type == "circle"){
        new paper.Shape.Circle({
            center: new paper.Point(p2[0], p2[1]),
            radius: 5,
            strokeColor: color
        });
    } else {
        console.error("Unknown arrow type: ",type);
    }
}

function drawLabel(obj) {
    var sx, sy, ex, ey;
    var maxLength = 0;
    for (var i = 0; i < obj.segments.length-1; i++) {
        var segment = obj.segments[i];
        var segment2 = obj.segments[i+1];

        var distance = Math.pow(segment2[0]-segment[0],2) + Math.pow(segment2[1]-segment[1],2);
        if (distance > maxLength) {
            maxLength = distance;
            sx = segment[0];
            sy = segment[1];
            ex = segment2[0];
            ey = segment2[1];
        }
    }

    var pointText = new paper.PointText({
        content: obj.label,
        fillColor: obj.textColor || obj.strokeColor
    });

    var textOffset = pointText.bounds.height / 2;

    if (ex < sx) {
        textOffset *= -1;
    }

    var middlePoint = new paper.Point((sx+ex)/2, (sy+ey)/2);
    pointText.position = middlePoint;
    pointText.position.y += textOffset;

    var group = new paper.Group(pointText);
    var angle = Math.atan((sy-ey) / (sx-ex)) * (180/Math.PI);

    if (ex < sx) {
        angle += 180;
        pointText.rotate(180);
    }

    group.rotate(angle, new paper.Point(middlePoint.x, middlePoint.y));
}

function shortVector(px0, py0, px1, py1) {
    //last point
    var vector = [
        px0 - px1,
        py0 - py1
    ];

    var length = Math.sqrt(Math.pow(vector[0],2) + Math.pow(vector[1],2));
    var shortLength = length - 5;
    var multiplier = shortLength / length;
    vector[0] *= (multiplier * - 1);
    vector[1] *= (multiplier * - 1);

    return [
        px0 + vector[0],
        py0 + vector[1]
    ];
}

function calcCone(px0, py0, px1, py1) {
    var vector = [
        px0 - px1,
        py0 - py1
    ];

    //rotate 90 degree
    var x0 = vector[1];
    var y0 = vector[0] * -1;

    //rotate 270 degree
    var x1 = vector[1] * -1;
    var y1 = vector[0];

    return [px1 + x0, py1 + y0, px1 + x1, py1 + y1];
}

function calcArrow(px0, py0, px, py) {
    var points = [];
    var l = Math.sqrt(Math.pow((px - px0), 2) + Math.pow((py - py0), 2)) * 1.2;
    points[0] = (px - ((px - px0) * Math.cos(0.5) - (py - py0) * Math.sin(0.5)) * 10 / l);
    points[1] = (py - ((py - py0) * Math.cos(0.5) + (px - px0) * Math.sin(0.5)) * 10 / l);
    points[2] = (px - ((px - px0) * Math.cos(0.5) + (py - py0) * Math.sin(0.5)) * 10 / l);
    points[3] = (py - ((py - py0) * Math.cos(0.5) - (px - px0) * Math.sin(0.5)) * 10 / l);
    return points;
}

function roundPath(path,radius) {

    var segments = path.segments.slice(0);
    path.segments = [];
    for(var i = 0, l = segments.length; i < l; i++) {
        var curPoint = segments[i].point;
        var nextPoint = segments[i + 1 == l ? 0 : i + 1].point;
        var prevPoint = segments[i - 1 < 0 ? segments.length - 1 : i - 1].point;
        var nextDelta = minus(curPoint, nextPoint);
        var prevDelta = minus(curPoint, prevPoint);
        nextDelta.length = radius;
        prevDelta.length = radius;

        new paper.Point(1,1);
        if (i > 0 && i < segments.length - 1) {
            path.add({
                point: minus(curPoint, prevDelta),
                handleOut: divide(prevDelta, 2)
            });
            path.add({
                point: minus(curPoint, nextDelta),
                handleIn: divide(nextDelta, 2)
            });
        } else {
            path.add({
                point: curPoint
            });
        }

    }

    return path;
}

function minus(p1, p2) {
    return new paper.Point(p1.x - p2.x, p1.y - p2.y);
}

function divide(p1, num) {
    return new paper.Point(p1.x / num, p1.y / num);
}

function drawRect(planCanvas, node) {
    node.shape = new planCanvas.Shape.Rectangle({
        point: new planCanvas.Point(node.x - node.width/2 + node.xOffset, node.y - node.height/2 + node.yOffset),
        size: new planCanvas.Size(node.width, node.height),
        strokeColor: node.strokeColor || GraphStyle.strokeColor,
        strokeWidth: node.strokeWidth || GraphStyle.strokeWidth,
        fillColor: node.fillColor || GraphStyle.fillColor,
        dashArray: node.dashArray
    });
    node.text.position = [node.x + node.xOffset, node.y + node.yOffset];
    node.text.bringToFront();

    if (node.tooltip) {
        drawToolTip(planCanvas, node, node.tooltip);
    }

    var group = new paper.Group([node.shape, node.text]);

    if (node.toolTipObject) {
        group.onMouseEnter = function(){
            node.toolTipObject.visible = true;
        };
        group.onMouseLeave = function(){
            node.toolTipObject.visible = false;
        };
    }
}

function drawInput(planCanvas, node) {
    if (node.tooltip) {
        drawToolTip(planCanvas, node, node.tooltip);
    }

    node.shape = planCanvas.Shape.Cylinder(node);
    node.text.position = [node.x + node.xOffset, node.y + node.yOffset + 5];
    node.text.bringToFront();
}

function drawToolTip(canvas, node, text) {
    var tooltipOffset = 10;
    var textSize = GraphStyle.fontSize - 4;

    var startX = node.x + node.width/2 + node.xOffset + tooltipOffset;
    var startY = node.y + node.yOffset - textSize/2;

    var tooltipText = new canvas.PointText({
        content: text,
        fillColor: GraphStyle.strokeColor,
        fontFamily: GraphStyle.font,
        fontSize: textSize,
        point:[startX, node.y + node.yOffset + 5]
    });

    tooltipText.wordwrap(text, 40);

    var background = new canvas.Shape.Rectangle({
        from:[startX - 1, startY - 1],
        to: [startX + tooltipText.bounds.width + 1, startY + tooltipText.bounds.height + 1],
        fillColor: GraphStyle.fillColor,
        strokeColor: node.strokeColor || GraphStyle.strokeColor,
        strokeWidth: node.strokeWidth || GraphStyle.strokeWidth
    });

    node.toolTipObject = new paper.Group([background, tooltipText]);
    node.toolTipObject.visible = false;
}

function drawConstant(planCanvas, node) {
    var radius = Math.sqrt(Math.pow(node.width/2, 2) + Math.pow(node.height/2, 2));
    node.shape = new planCanvas.Shape.Circle({
        center: [node.x + node.xOffset, node.y + node.yOffset],
        radius: radius,
        strokeColor: node.strokeColor || GraphStyle.strokeColor,
        strokeWidth: node.strokeWidth || GraphStyle.strokeWidth,
        fillColor: node.fillColor || GraphStyle.fillColor,
        dashArray: node.dashArray
    });
    node.text.position = [node.x + node.xOffset, node.y + node.yOffset];
    node.text.bringToFront();
}

paper.Shape.Cylinder = function (object) {
    //lower ellipse
    var lowerEllipse = new paper.Shape.Ellipse({
       center: [object.x + object.xOffset, object.y + object.height/2 + object.yOffset],
       radius: [object.width / 2, 5],
       fillColor: object.fillColor || GraphStyle.fillColor,
       strokeColor: object.strokeColor || GraphStyle.strokeColor,
       strokeWidth: object.strokeWidth || GraphStyle.strokeWidth,
       dashArray: object.dashArray
    });

    var rect = paper.Shape.Rectangle({
       point: new paper.Point(object.x - object.width/2 + object.xOffset, object.y - object.height/2 + object.yOffset + 5),
       size: new paper.Size(object.width, object.height - 5),
       strokeWidth: object.strokeWidth || GraphStyle.strokeWidth,
       fillColor: object.fillColor || GraphStyle.fillColor
    });

    //border lines
    var leftLine = new paper.Path.Line({
        segments: [
            new paper.Point(object.x - object.width/2 + object.xOffset, object.y + object.yOffset - object.height / 2 + 5),
            new paper.Point(object.x - object.width/2 + object.xOffset, object.y + object.yOffset + object.height / 2)
        ],
        strokeColor: object.strokeColor || GraphStyle.strokeColor,
        strokeWidth: object.strokeWidth || GraphStyle.strokeWidth,
        dashArray: object.dashArray || null
    });

    var rightLine = new paper.Path.Line({
        segments: [
            new paper.Point(object.x + object.width/2 + object.xOffset, object.y + object.yOffset - object.height / 2 + 5),
            new paper.Point(object.x + object.width/2 + object.xOffset, object.y + object.yOffset + object.height / 2)
        ],
        strokeColor: object.strokeColor || GraphStyle.strokeColor,
        strokeWidth: object.strokeWidth || GraphStyle.strokeWidth,
        dashArray: object.dashArray || null
    });

    //upper ellipse
    var upperEllipse = new paper.Shape.Ellipse({
       center: [object.x + object.xOffset, object.y + object.yOffset - object.height/2 + 5],
       radius: [object.width/2, 5],
       fillColor: object.fillColor || GraphStyle.fillColor,
       strokeColor: object.strokeColor || GraphStyle.strokeColor,
       strokeWidth: object.strokeWidth || GraphStyle.strokeWidth,
       dashArray: object.dashArray || null
    });

    var group = new paper.Group([lowerEllipse, rect, leftLine, rightLine, upperEllipse, object.text]);

    if (object.toolTipObject) {
        group.onMouseEnter = function(){
            object.toolTipObject.visible = true;
        };

        group.onMouseLeave = function(){
            object.toolTipObject.visible = false;
        };
    }
    return group;
};