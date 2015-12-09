var IsDrag = false,
    lastX = 0,
    lastY = 0,
    upperZoomLimit = 10,
    lowerZoomLimit = 0.3;

function drawPlan(plan, id) {
    var canvas = setupPlanCanvas(id);
    resizeRightView();
    initGraph(plan, id);
    canvas.view.draw();
}

function setupPlanCanvas(id) {
    var canvasId = "plan-canvas"+id;

    if (canvases["#"+canvasId] == undefined) {
        var canvasDomElement = $("#"+canvasId);
        var planCanvas = new paper.PaperScope().setup(document.getElementById(canvasId));

        var project = planCanvas.project;

        var tool = new paper.Tool();

        tool.distanceThreshold = 8;
        tool.mouseStartPos = new paper.Point();
        tool.zoomFactor = 1.5;

        //zoom view
        canvasDomElement.bind('mousewheel DOMMouseScroll MozMousePixelScroll', function(e){
            var delta = 0;
            var children = project.activeLayer.children;

            e.preventDefault();
            e = e || window.event;
            if (e.type == 'mousewheel') {       //this is for chrome/IE
                delta = e.originalEvent.wheelDelta;
            } else if (e.type == 'DOMMouseScroll') {  //this is for FireFox
                delta = e.originalEvent.detail*-1;  //FireFox reverses the scroll so we force to to re-reverse...
            }

            if ((delta > 0) && (paper.view.zoom < upperZoomLimit)) {
                //scroll up

                var point = new paper.Point(e.originalEvent.offsetX, e.originalEvent.offsetY);
                point = paper.view.viewToProject(point);
                var zoomCenter = point.subtract(paper.view.center);
                var moveFactor = tool.zoomFactor - 1.0;
                paper.view.zoom *= tool.zoomFactor;
                paper.view.center = paper.view.center.add(zoomCenter.multiply(moveFactor / tool.zoomFactor));
                tool.mode = '';

            } else if((delta < 0) && (paper.view.zoom>lowerZoomLimit)){ //scroll down
                var point = new paper.Point(e.originalEvent.offsetX, e.originalEvent.offsetY);
                point = paper.view.viewToProject(point);
                var zoomCenter = point.subtract(paper.view.center);
                var moveFactor = tool.zoomFactor - 1.0;
                paper.view.zoom /= tool.zoomFactor;
                paper.view.center = paper.view.center.subtract(zoomCenter.multiply(moveFactor))
            }
        });

        //pan view
        tool.onMouseDrag = function (event) {
            var canvasOffset = canvasDomElement.offset();
            if (event.event.x != undefined) {
                var x = event.event.x;
                var y = event.event.y;
            } else {
                var x = event.event.clientX;
                var y = event.event.clientY;
            }
            x -= canvasOffset.left;
            y -= canvasOffset.top;


            if (event.tool._count == 1) {
               lastX = x;
               lastY = y;
               window.document.body.style.cursor = 'move';
               IsDrag = true;
            }

            var point = new paper.Point(lastX - x, lastY - y);

            point = point.multiply( 1 / paper.view.zoom);

            planCanvas.project.view.scrollBy(point);
            lastX = x;
            lastY = y;
        }

        //pan view
        tool.onMouseUp = function (event) {
            if (IsDrag == true) {
                // reset
                IsDrag = false;
                window.document.body.style.cursor = 'default';
            }
        }

        initWordWrap(planCanvas);
        canvases["#"+canvasId] = planCanvas;
    } else {
        planCanvas = canvases["#"+canvasId];
    }
    return planCanvas;
}

function initWordWrap(planCanvas) {
    planCanvas.PointText.prototype.wordwrap = function(text, maxChar){
        var lines = [],
            space = -1,
            times = 0;

        function cut(){
            for (var i = 0; i < text.length; i++){
                (text[i] == ' ') && (space = i);

                if(i >= maxChar){
                    (space == -1 || text[i] == ' ') && (space = i);

                    if(space>0)
                        lines.push(text.slice((text[0] == ' ' ? 1 : 0),space));

                    text = text.slice(text[0] == ' ' ? (space+1) : space);
                    space = -1;
                    break;
                }
            }
            check();
        }

        function check(){
            if (text.length <= maxChar){
                lines.push(text[0] == ' ' ? text.slice(1) : text);
                text='';
            }else if(text.length){
                cut();
            }
            return;
        }

        check();
        return this.content = lines.join('\n');
    }
}