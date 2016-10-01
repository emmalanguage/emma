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
var CodeView = function() {
    return {
        codeHasCarriageReturns: false,
        codeStartIndex: 0,
        codeCanvas: null,
        iterationMarker: [],
        currentComprehensions: {},
        comprehensionHighlightColor: 'rgba(255,0,0,0.4)',

        init: function() {
            $('a[href="#code-container"]').click();
            $('#code-container').scrollTop(0);
            this.iterationMarker = [];
            if (this.codeCanvas != null) {
                this.codeCanvas.project.activeLayer.removeChildren();
                this.codeCanvas.view.draw();
            }
        },

        setCodeHasCarriageReturns: function(bool) {
            this.codeHasCarriageReturns = bool;
        },

        setupCodeCanvas: function() {
            if (this.codeCanvas == null) {
                this.codeCanvas = new paper.PaperScope().setup($('#code-canvas')[0]);
            }
        },

        renderCode: function(sourceCode){
            var container = $('#code-container');
            var code = container.find('code');
            var codeCanvasElement = container.find('canvas');

            code.html(this.filterParallelizeFunction(sourceCode));
            code.each(function(i, block) {
                hljs.highlightBlock(block);
            });

            var contentWidth = code[0].scrollWidth;
            code.width(contentWidth);
            codeCanvasElement.width(contentWidth);
            codeCanvasElement.height(code.height()+20);
            code.parent().width(contentWidth);
            this.updateComprehensionBoxes();
        },

        filterParallelizeFunction: function(code) {
            this.codeStartIndex = code.indexOf('emma.parallelize');

            while (code[--this.codeStartIndex] != '\n') {}

            this.codeStartIndex++;

            var bracesCount = 0;
            var start = false;
            var index = this.codeStartIndex;
            while (!start || bracesCount != 0) {
                if (!start && code[index] == '{')
                    start = true;

                if (code[index] == '{')
                    bracesCount++;

                if (code[index] == '}')
                    bracesCount--;

                index++;
            }

            return code.substr(this.codeStartIndex, index-this.codeStartIndex);
        },

        buildParameterForm: function(parameters) {
            if (parameters) {
                var parameterContainer = $('#parameters');
                parameterContainer.html("");
                for (var param in parameters) {
                    var parameter = parameters[param];
                    var row = $('<div></div>').addClass("row");
                    var column = $('<div></div>').addClass("small-12 columns");
                    var label = $('<label></label>').html(param+":");
                    var input = $('<input>')
                        .attr("type","text")
                        .attr("name",param)
                        .val(parameter);
                    label.append(input);
                    column.append(label);
                    row.append(column);
                    parameterContainer.append(row);
                }

                $('#submit-parameters').show();

            } else {
                console.error("no input parameters");
            }
        },

        drawComprehensionBoxes: function(name) {
            var boxes = this.currentComprehensions[name];
            this.codeCanvas = new paper.PaperScope().setup($('#code-canvas')[0]);

            if (boxes != null && boxes.length > 0) {
                var minStart = Number.MAX_VALUE;
                var self = this;
                boxes.forEach(function(box){
                    var boxPos = self.drawComprehensionBox(box[0] - self.codeStartIndex, box[1] - self.codeStartIndex);
                    if (boxPos != null && boxPos.top < minStart) {
                        minStart = boxPos.top;
                    }
                });

                if (minStart != Number.MAX_VALUE) {
                    if (scrollCode) {
                        $('#code-container').finish().animate({
                            scrollTop: minStart - 20
                        }, 400, "easeOutCirc");
                    }
                }
            }

            if (this.iterationMarker.length > 0) {
                var maxWidth = $('#code-canvas').width();
                for (var i in this.iterationMarker) {
                    var iteration = this.iterationMarker[i];
                    var clientRect = this.setSelectionRange($('#code-container').find('code')[0], iteration[0] - this.codeStartIndex, iteration[1] - this.codeStartIndex);
                    if (clientRect != null && this.codeCanvas) {
                        var margin = 3;
                        var rectangle = new this.codeCanvas.Shape.Rectangle(
                            new this.codeCanvas.Point(margin, clientRect.top - margin),
                            maxWidth - 2*margin,
                            clientRect.height + margin
                        );
                        rectangle.strokeColor = iterationColors[i%iterationColors.length];
                        rectangle.strokeWidth = 2;
                    }
                }
            }

            this.codeCanvas.view.draw();
        },

        drawComprehensionBox: function(begin, end) {
            var margin = 4;
            var clientRect = this.setSelectionRange($('#code-container').find('code')[0], begin, end);

            if (clientRect != null) {
                var maxWidth = $('#code-canvas').width();
                var rectangle = new this.codeCanvas.Shape.Rectangle(
                    new this.codeCanvas.Point(margin, clientRect.top - margin),
                    maxWidth - 2*margin,
                    clientRect.height + margin
                );
                rectangle.fillColor = this.comprehensionHighlightColor;
                return {
                    left: margin,
                    top: clientRect.top - margin,
                    width: maxWidth - 2*margin,
                    height: clientRect.height + margin
                };
            }

            return null;
        },

        updateComprehensionBoxes: function() {
            this.updateCodeCanvasSize();
            var comprehensionName = $('#plan-tabs').find('li.active a').attr("plan-name");
            this.drawComprehensionBoxes(comprehensionName);
        },

        updateCodeCanvasSize: function() {
            var codeContainer = $('#code-tab-wrapper').find(".tabs-content .active");
            $('#code-canvas').css("width", 1)
                .css("height", 1)
                .css("width", codeContainer.prop("scrollWidth"))
                .css("height", codeContainer.prop("scrollHeight"));
        },

        setSelectionRange: function(el, start, end) {

            if (document.createRange && window.getSelection) {
                var range = document.createRange();
                var textNodes = this.getTextNodesIn(el);
                var foundStart = false;
                var charCount = 0, from, to;

                for (var i = 0, textNode; textNode = textNodes[i++]; ) {
                    var text = $(textNode).text();

                    from = charCount;

                    charCount += text.length;

                    var newLineCount = 0;
                    if (this.codeHasCarriageReturns) {
                        newLineCount = text.split("\n").length - 1;
                        charCount += newLineCount;
                    }

                    to = charCount;

                    if (!foundStart && start >= from && start <= to) {
                        if (start - from - newLineCount <= textNode.length) {
                            range.setStart(textNode, start - from - newLineCount);
                            foundStart = true;
                        } else {
                            console.warn("Text selection out of range. Node length: ",textNode.length, "Index: ",start - from, "Start: ", start, "End: ", end, "CodeStartIndex: ", this.codeStartIndex);
                        }
                    }

                    if (foundStart && end >= from && end <= to) {
                        while (end - from > textNode.length) {
                            from++;
                        }
                        range.setEnd(textNode, end - from);
                        break;
                    }
                }

                var codeContainer = $(el).parents('#code-container');
                var offset = codeContainer.offset();
                var clientRect = range.getBoundingClientRect();

                //show selected text
                //var sel = window.getSelection();
                //sel.removeAllRanges();
                //sel.addRange(range);

                return (range.getClientRects().length > 0) ?
                {
                    top: clientRect.top - offset.top + codeContainer.scrollTop() + 1,
                    left: range.getClientRects()[0].left - offset.left + codeContainer.scrollLeft(),
                    width: clientRect.width,
                    height: clientRect.height || 19
                }: null;
            } else {
                console.warn("Range selection not supported. Comprehensions can not be shown.");
            }
        },

        getTextNodesIn: function(node) {
            var textNodes = [];
            if (node.nodeType == 3) {
                textNodes.push(node);
            } else {
                var children = node.childNodes;
                for (var i = 0, len = children.length; i < len; ++i) {
                    textNodes.push.apply(textNodes, this.getTextNodesIn(children[i]));
                }
            }
            return textNodes;
        },

        registerEvents: function(){
            //code tab click
            var self = this;
            $('#code-tabs').on('toggled', function (event, tab) {
                if (tab.find('a').attr('href') == '#code-container') {
                    self.updateComprehensionBoxes();
                }
            });

            //zoom button click
            $('#code-zoom-in').click(function(){
                var codeContainer = $('#code-container');
                var fontSize = parseInt(codeContainer.css('font-size').replace('px',''));
                codeContainer.css('font-size',(fontSize+fontSizeStep)+"px");
                self.updateComprehensionBoxes();
            });

            //zoom button click
            $('#code-zoom-out').click(function(){
                var codeContainer = $('#code-container');
                var fontSize = parseInt(codeContainer.css('font-size').replace('px',''));
                codeContainer.css('font-size',(fontSize-fontSizeStep)+"px");
                self.updateComprehensionBoxes();
            });

            //parameter rerun button
            $('#submit-parameters').click(function(){
                prepareRerun(parseParameters());
                return false;
            });

            //change code scroll state
            $('#scroll-code').change(function(){
                scrollCode = this.checked;
                localStorage.setItem("scrollCode", scrollCode);
            });
        },

        resizeView: function() {
            var codeWrapper = $(".code-wrapper");
            codeWrapper.resizable({
                minWidth: codeWrapper.parent().width()*0.2,
                maxWidth: codeWrapper.parent().width()*0.8,
                handles: 'e'
            });

            var wrapper = $("#code-tab-wrapper");
            var handleHeight = wrapper.find('.ui-resizable-handle').height();

            var codeTabs = $('#code-tabs');

            var maxHeight = $("html").height()
                - wrapper.offset().top
                - codeTabs.height()
                - handleHeight
                - $('#log-options').height()
                - 14;   //margin bottom

            if (!handleHeight) {
                maxHeight -= 7; //inconsistent ui-resizable-handle height
                maxHeight += 1; //inconsistent log-options height
            }

            var wrapperHeight = parseInt(wrapper.css("height").replace("px",""));

            if (wrapperHeight > maxHeight) {
                wrapper.height(maxHeight*codeHeightMaxFactor);
            }

            //init height
            if ($('#code-tab-wrapper[style*="height"]').length == 0) {
                wrapper.css('height', maxHeight*codeHeightMaxFactor);
            }

            var codeHeight = wrapper.height() - codeTabs.height();
            var logHeight = maxHeight - codeHeight;

            var codeTabContents = wrapper.find('.tabs-content .content');

            codeTabContents.each(function(i,e){
                var container = $(e);
                if (codeTabs.find('li').css('borderBottomWidth').replace('px','') == 0) {
                    //firefox fix
                    container.css("height", codeHeight - 3);
                } else {
                    container.css("height", codeHeight);
                }
            });

            $('#log-container').css("height", logHeight);

            wrapper.resizable({
                minHeight: maxHeight*codeHeightMinFactor,
                maxHeight: maxHeight*codeHeightMaxFactor,
                handles: 's'
            });
        }
    };
};

