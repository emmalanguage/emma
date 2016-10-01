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
var PlanView = function(){

    var planView = {
        canvases: {},
        tabCount: 0,
        upperZoomLimit: 10,
        lowerZoomLimit: 0.3,
        fadeSpeed: 1000,

        init: function() {
            this.canvases = {};
            this.tabCount = 0;
            $('#plan-tabs').html("");
            $('#plan-tab-content').html("No example selected");
        },

        buildPlan: function(data) {
            addToExecutionOrder(currentExecution, data.graph.name);

            currentExecution = data.graph.name;
            if (!(currentExecution in planNames)) {
                var planIndex = Object.keys(planNames).length;
                planNames[currentExecution] = {
                    index: planIndex,
                    executed: 0
                };
                planCache.push(data);
                this.addTab(data.graph);

                this.drawPlan(data.graph, this.tabCount-1);

                localStorage.setItem(loadedExample, JSON.stringify(planCache));
            }
            $('#plan-canvas'+(this.tabCount-1)).fadeIn(this.fadeSpeed);
        },

        drawPlan: function(plan, id) {
            var canvas = this.setupPlanCanvas(id);
            this.resizeView();
            initGraph(plan, id, this.canvases['#plan-canvas'+id]);
            canvas.view.draw();
        },

        addTab: function(plan) {
            var shortName = plan.name.substr(0, plan.name.indexOf('$'));

            var executed = 0;
            if (planNames[plan.name])
                executed = planNames[plan.name].executed;

            if (executed > 1)
                shortName += "("+executed+")";

            $('#plan-tabs').append('<li class="tab-title '+((this.tabCount == 0)?'active':'')+'"><a href="#panel'+(this.tabCount)+'" plan-name="'+plan.name+'">'+shortName+'</a></li>');
            var content = $('<div class="content '+((this.tabCount == 0)?'active':'')+'" id="panel'+(this.tabCount)+'"><canvas id="plan-canvas'+(this.tabCount)+'" style="display: none;"></canvas></div>');
            content.attr("aria-hidden","true");
            $('#plan-tab-content').append(content);
            this.tabCount++;
        },

        scrollToTab: function(tab) {
            var planTabs = $('#plan-tabs');
            var middlePoint = planTabs.width() / 2 - tab.width() / 2;
            var scrollPosition = tab.offset().left - tab.parent().offset().left + tab.parent().scrollLeft() - middlePoint;
            planTabs.finish().animate({
                scrollLeft: scrollPosition
            }, 400, "easeOutCirc");
        },

        updateTabLabel: function(planTab, name, planMetaData) {
            var shortName = name.substr(0, name.indexOf('$'));

            if (planMetaData.executed > 1)
                planTab.html(shortName+"("+(planMetaData.executed)+")");
            else
                planTab.html(shortName);
        },

        resizeView: function() {
            var wrapper = $("#tab-wrapper");
            var content = $('#plan-tab-content');
            var height = $("html").height() - wrapper.offset().top - 11;
            var width = content.width();
            wrapper.css("height", height);

            height -= $('#plan-tabs').height();
            content.find('.content').each(function(i, tabContent){
                $(tabContent).css("height", height);
            });

            for(var id in this.canvases) {
                var canvas = this.canvases[id];
                canvas.view.viewSize = [width, height];
                canvas.view.update(true);
            }
        },

        initWordWrap: function(planCanvas) {
            planCanvas.PointText.prototype.wordwrap = function(text, maxChar){
                var lines = [],
                    space = -1;

                function cut(){
                    for (var i = 0; i < text.length; i++){
                        (isWhiteSpace(text[i])) && (space = i);

                        if(i >= maxChar){
                            (space == -1 || isWhiteSpace(text[i])) && (space = i);

                            if(space>0)
                                lines.push(text.slice((isWhiteSpace(text[0]) ? 1 : 0),space));

                            text = text.slice(isWhiteSpace(text[0]) ? (space+1) : space);
                            space = -1;
                            break;
                        }
                    }
                    check();
                }

                function check(){
                    if (text.length <= maxChar){
                        lines.push(isWhiteSpace(text[0]) ? text.slice(1) : text);
                        text='';
                    }else if(text.length){
                        cut();
                    }
                }

                function isWhiteSpace(char) {
                    return char.match(/\s/g) != null;
                }

                check();
                return this.content = lines.join('\n');
            }
        },

        setupPlanCanvas: function(id) {
            var canvasId = "plan-canvas"+id;

            if (this.canvases["#"+canvasId] == undefined) {
                var canvasDomElement = $("#"+canvasId);
                var planCanvas = new paper.PaperScope().setup(document.getElementById(canvasId));

                var tool = new paper.Tool();

                tool.distanceThreshold = 8;
                tool.mouseStartPos = new paper.Point();
                tool.zoomFactor = 1.5;

                var self = this;

                //zoom view
                canvasDomElement.bind('mousewheel DOMMouseScroll MozMousePixelScroll', function(e){
                    var delta = 0;
                    e.preventDefault();
                    e = e || window.event;
                    if (e.type == 'mousewheel') {       //this is for chrome/IE
                        delta = e.originalEvent.wheelDelta;
                    } else if (e.type == 'DOMMouseScroll') {  //this is for FireFox
                        delta = e.originalEvent.detail*-1;
                    }

                    var point = new paper.Point(e.originalEvent.offsetX, e.originalEvent.offsetY);
                    point = paper.view.viewToProject(point);
                    var zoomCenter = point.subtract(paper.view.center);
                    var moveFactor = tool.zoomFactor - 1.0;
                    if ((delta > 0) && (paper.view.zoom < self.upperZoomLimit)) {
                        //scroll up
                        planCanvas.view.zoom *= tool.zoomFactor;
                        planCanvas.view.center = planCanvas.view.center.add(zoomCenter.multiply(moveFactor / tool.zoomFactor));
                        tool.mode = '';
                    } else if((delta < 0) && (paper.view.zoom> self.lowerZoomLimit)){ //scroll down
                        planCanvas.view.zoom /= tool.zoomFactor;
                        planCanvas.view.center = planCanvas.view.center.subtract(zoomCenter.multiply(moveFactor))
                    }
                });

                var lastX,
                    lastY,
                    isDrag = false;

                //pan view
                tool.onMouseDrag = function (event) {
                    //var lastX, lastY;

                    var canvasOffset = canvasDomElement.offset();
                    var x,y;
                    if (event.event.x != undefined) {
                        x = event.event.x;
                        y = event.event.y;
                    } else {
                        x = event.event.clientX;
                        y = event.event.clientY;
                    }
                    x -= canvasOffset.left;
                    y -= canvasOffset.top;

                    if (event.tool._count == 1) {
                        lastX = x;
                        lastY = y;
                        window.document.body.style.cursor = 'move';
                        isDrag = true;
                    }

                    var point = new paper.Point(lastX - x, lastY - y);

                    point = point.multiply( 1 / paper.view.zoom);

                    planCanvas.project.view.scrollBy(point);
                    lastX = x;
                    lastY = y;
                };


                //pan view
                tool.onMouseUp = function () {
                    if (isDrag == true) {
                        // reset
                        isDrag = false;
                        window.document.body.style.cursor = 'default';
                    }
                };

                this.initWordWrap(planCanvas);
                this.canvases["#"+canvasId] = planCanvas;
            } else {
                planCanvas = this.canvases["#"+canvasId];
            }
            return planCanvas;
        },

        updateCanvases: function() {
            for(var id in this.canvases) {
                this.canvases[id].view.update(true);
            }
        },

        markIterations: function(name, codeView){
            var minSelection = Number.MAX_VALUE;
            var maxSelection = 0;

            var iterations = JSON.parse(localStorage.getItem(name+"_iterations"));
            if (iterations && iterations.length > 0) {
                for (var i in iterations) {
                    var iteration = iterations[i];
                    var color = iterationColors[i%iterationColors.length];
                    for (var j in iteration) {
                        var planName = iteration[j];
                        $('li a[plan-name="'+planName+'"]').css("background-color", color);
                        if (codeView.currentComprehensions[planName]) {
                            if (codeView.currentComprehensions[planName][0][0] < minSelection)
                                minSelection = codeView.currentComprehensions[planName][0][0];
                            if (codeView.currentComprehensions[planName][0][1] > maxSelection)
                                maxSelection = codeView.currentComprehensions[planName][0][1];
                        }
                    }
                    codeView.iterationMarker[i] = [minSelection, maxSelection];
                }
            } else {
                codeView.iterationMarker = [];
            }
        },

        keyEvent: function(key) {
            if (key == 39) {
                //select to next tab
                $('#plan-tabs').find('li.active').next().find('a').click();
            }

            if (key == 37) {
                //select to previous tab
                $('#plan-tabs').find('li.active').prev().find('a').click();
            }
        }
    };

    //init
    //plan tab click
    $('#plan-tabs').on('toggled', function (event, tab) {
        var planCanvasId = tab.find('a').attr('href').replace('#panel','#plan-canvas');
        $(planCanvasId).hide().fadeIn(this.fadeSpeed);
        planView.scrollToTab(tab);

        codeView.updateComprehensionBoxes();
    });

    return planView;
};