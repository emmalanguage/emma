var strokeColor = '#F55B11',
    strokeWidth = 1,
    font = 'Courier New',
    fontSize = 18,
    fillColor = 'black',
    globalXOffset = 0,
    globalYOffset = 0;

var graphData = null;

var Node = function(obj, planCanvas){
    var node = $.extend(obj, {
        width: 10,
        height: 10,
        x: 0,
        y: 0,
        xOffset: 0,
        yOffset: 0,
        padding: [5,5,5,5],
        shape: null,

        draw: function() {
            if (this.shape == null) {
                this.xOffset = globalXOffset;
                this.yOffset = globalYOffset;

                switch (this.type) {
                    case "INPUT": drawInput(planCanvas, this); break;
                    case "CONSTANT": drawConstant(planCanvas, this); break;
                    default: drawRect(planCanvas, this)
                }
            } else {
                //TODO: implement position update
                console.error("position update not implemented");
            }
        },

        setLabel: function(label) {
            if (this.text != null) {
                label = label+"";
                if (label.length <= 1) {
                    var maxLength = 1;
                } else {
                    var maxLength = Math.ceil(Math.sqrt(label.length) * 2) + 5;
                    if (maxLength < 10) {
                        maxLength = 10;
                    }
                }
                this.text.wordwrap(label, maxLength);
                this.text.justification = "center";

                this.width = this.text.bounds.width + this.padding[1] + this.padding[3];
                this.height = this.text.bounds.height + this.padding[0] + this.padding[2];
            }
        },
    });

    //init
    node.text = new planCanvas.PointText({
        position: [node.x, node.y],
        content: "",
        fillColor: node.textColor || strokeColor,
        fontFamily: font,
        fontSize: fontSize
    });

    node.setLabel(node.label);

    node.width = node.text.bounds.width + node.padding[1] + node.padding[3];
    node.height = node.text.bounds.height + node.padding[0] + node.padding[2];

    return node;
}

var Edge = function(obj) {
    var edge = $.extend(obj,
    {
        xOffset: 0,
        yOffset: 0,

        draw: function() {
            var segments = [];
            xOffset = this.xOffset;
            yOffset = this.yOffset;

            xOffset += globalXOffset;
            yOffset += globalYOffset;

            var startArrow = null;
            var endArrow = null;

            this.points.forEach(function(point){
                segments.push([point.x + xOffset, point.y + yOffset]);
            });

            if (this.type == "BROADCAST") {
                this.label = "broadcast";
            }

            if (this.type == "BROADCAST" || this.type == "REPARTITION") {
                endArrow = "circle";
            }

            if (this.type == "REPARTITION") {
                startArrow = "circle";
                this.label = "repartition"
            }

            if (this.type == null) {
                endArrow = "arrow";
            }

            var path = arrow({
                label: this.label,
                segments: segments,
                strokeColor: this.strokeColor || strokeColor,
                strokeWidth: this.strokeWidth || strokeWidth,
                dashArray: this.dashArray || 1,
                strokeCap: this.strokeCap,
                strokeJoin: this.strokeJoin,
                textColor: this.textColor,
                startArrow: startArrow,
                endArrow: endArrow
            });

            path = roundPath(path, 5);
        }
    });
    return edge;
}

var nodes = [];
var edges = [];

function initGraph(plan, id) {

    var g = new dagre.graphlib.Graph();
    g.setGraph({});
    g.setDefaultEdgeLabel(function() { return {}; });

    graphData = plan;

    if (graphData == null)
        return;

    var planCanvas = canvases['#plan-canvas'+id];

    graphData.nodes.forEach(function(node, i){
        g.setNode(node.id, new Node(node, planCanvas));
    });

    graphData.edges.forEach(function(edge){
        g.setEdge(edge.connect[0], edge.connect[1], edge);
    });

    dagre.layout(g);

    //compute middle point
    var middlePoint = [];
    middlePoint[0] = $('#plan-canvas'+id).attr("width") / 2;
    middlePoint[1] = $('#plan-canvas'+id).attr("height") / 2;

    var dimensions = getGraphDimensions(g);

    globalXOffset = middlePoint[0] - dimensions[0] / 2;
    globalYOffset = middlePoint[1] - dimensions[1] / 2;

    g.edges().forEach(function(e) {
        var edge = new Edge(g.edge(e));
        edge.draw();
        edges.push(edge);
    });

    g.nodes().forEach(function(v) {
        var node = g.node(v);
        node.draw();
        nodes.push(node);
    });

    nodes.forEach(function(node){
        if (node.toolTipObject) {
            node.toolTipObject.bringToFront();
        }
    });
}

function getGraphDimensions(g) {
    var minX = 1000,
        minY = 1000,
        maxX = 0,
        maxY = 0;

    g.edges().forEach(function(e) {
        var edge = g.edge(e);
        edge.points.forEach(function(point){
            if (point.x < minX)
                minX = point.x;

            if (point.x > maxX)
                maxX = point.x;

            if (point.y < minY)
                minY = point.y;

            if (point.y > maxY)
                maxY = point.y;
        });
    });

    g.nodes().forEach(function(v) {
        var node = g.node(v);
        if (node.x < minX)
            minX = node.x;

        if (node.x + node.width > maxX)
            maxX = node.x + node.width;

        if (node.y < minY)
            minY = node.y;

        if (node.y + node.height > maxY)
            maxY = node.y + node.height;
    });

    return [maxX - minX, maxY - minY];
}