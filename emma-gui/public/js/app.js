'use strict';

var codeCanvas = null;
var codeStartIndex = 0;
var canvases = {};
var tabCount = 0;
var requestBase = "http://localhost:8080/";
var fullExampleName = "";
var planNames = {};
var planCache = [];
var currentExecution = "";
var executionOrder = [];
var loadedExample = "";
var planCaching = true;
var scrollLock = true;
var currentComprehensions = {};
var comprehensionHighlightColor = 'rgba(255,0,0,0.4)';
var fadeSpeed = 1000;
var fontSizeStep = 2;
var codeHeightMinFactor = 0.2;
var codeHeightMaxFactor = 0.9;
var logBufferSize = 500;
var fastForwardRun = true;
var checkLogSizeIteration = 0;
var iterationMarker = [];
var iterationColors = [
    '#24E0FB',
    '#DFDF00',
    '#3923D6',
    '#BBBBFF',
    '#01F33E',
    '#80B584'
];

registerListeners();

$("#code-tab-wrapper").resize(function(){
    resizeLeftView();
});

$(".code-wrapper").resize(function(){
    resizeRightView();
});

function clearPage(){
    var code = $('#code-container').find('code');
    code.html("");
    code.width("auto");
    code.parent().width("auto");

    init();

    $('#code-canvas').css("height", "0px");
}

function init() {
    canvases = {};
    tabCount = 0;
    planNames = {};
    planCache = [];
    currentExecution = "";
    executionOrder = [];
    loadedExample = "";

    $('#plan-tabs').html("");
    $('#plan-tab-content').html("No example selected");
    $('#log-container').html("");
    $('a[href="#code-panel0"]').click();

    setInitState();
    $('#example-name').html("");

    if (codeCanvas != null) {
        codeCanvas.project.activeLayer.removeChildren();
        codeCanvas.view.draw();
    }
}

var handleError = function(jqXHR, textStatus, errorThrown) {
    console.error(textStatus, errorThrown, jqXHR);
    setWaitingState();
    addLogHtml("<div class='log-error'>ERROR: status: '"+textStatus+"', errorThrown: '"+errorThrown+"'</div>");
    alert("An error occurred! Detailed information in the log.");
};

function loadExamples() {
    $.ajax({
        method: "GET",
        url: requestBase + "code/getExamples",
        success: function(data) {
            if (data != null) {
                data.forEach(function(example){
                    $('#exampleList').append('<li><a title="'+example.key+'">'+example.value+'</a></li>');
                });

                $('#exampleList').find('li a').click(function(e){
                    var exampleName = $(e.target).attr('title');
                    loadExample(exampleName);
                });
            }
        },
        error: handleError
    });
}

function loadExample(name) {
    fullExampleName = name;
    var code = $('#code-container').find('code');
    code.html("");
    init();

    var cache = localStorage.getItem(name);

    var exampleName = $("a[title='"+name+"']").text();
    if (exampleName) {
        $('#example-name').html(exampleName+((planCaching && cache != null)?' (cached)':''));
    } else {
        $('#example-name').html("");
    }

    $('#status').html('<i class="gear"/> loading plan...');

    loadCode(name);

    if (planCaching && cache != null) {
        loadPlanFromCache(name, cache);
    } else {
        loadPlan(name);
    }
}

function loadCode(name) {
    setupCodeCanvas();
    var code = $('#code-container').find('code');

    $.ajax({
        method: "GET",
        url: requestBase+"code/"+name,
        async: false,
        success: function(data) {
            if (data != null) {
                var codeCanvasElement = $('#code-container').find('canvas');

                code.html(filterParallelizeFunction(data.code));
                code.each(function(i, block) {
                    hljs.highlightBlock(block);
                });

                var contentWidth = code[0].scrollWidth;
                code.width(contentWidth);
                codeCanvasElement.width(contentWidth);
                codeCanvasElement.height(code.height()+20);
                code.parent().width(contentWidth+20);

                //TODO load from plan, comprehensions
                currentComprehensions = {};
                /*
                if (data.comprehensions != null) {
                    currentComprehensions = data.comprehensions;
                }
                */
            } else {
                console.log("No sources available. Did you compile the sources of emma-examples?");
                code.html("No sources available.");
            }
        },
        error: handleError
    });
}

function loadPlan(name) {
    $.ajax({
        method: "GET",
        url: requestBase+"plan/loadGraph?name="+name,
        success: function(data) {
            if (data.graph != null) {
                loadedExample = name;
                $('#plan-tab-content').html("");
                buildPlan(data);

                $('#plan-tabs').find('li:last a').click();

                if (!data.isLast) {
                    setWaitingState();
                }

            } else {
                console.error("Requested plan data is null! Request: "+requestBase+"plan/loadGraph?name="+name);
                $('#plan-tab-content').html("Requested plan cannot be loaded.");
                $('#status').html('');
            }
        },
        error: handleError
    });
}

function loadPlanFromCache(name, cache) {
    var plans = JSON.parse(cache);
    loadedExample = name;
    $('#plan-tab-content').html("");

    plans.forEach(function(plan){
        buildPlan(plan);
    });

    markIterations(name);

    for(var id in canvases) {
        canvases[id].view.update(true);
    }
    initRuntime(name);

    currentExecution = planCache[0].graph.name;
    planNames[currentExecution].executed = 0;
    setWaitingState();
    $('a[href="#panel0"]').click();
}

function initRuntime(name) {
    $.ajax({
        method: "GET",
        url: requestBase+"plan/initRuntime?name="+name,
        error: handleError
    });
}

function buildPlan(data) {
    addToExecutionOrder(currentExecution, data.graph.name);

    currentExecution = data.graph.name;
    if (!(currentExecution in planNames)) {
        var planIndex = Object.keys(planNames).length;
        planNames[currentExecution] = {
            index: planIndex,
            executed: 0
        };
        planCache.push(data);
        addTab(data.graph);

        drawPlan(data.graph, tabCount-1);

        localStorage.setItem(loadedExample, JSON.stringify(planCache));
    }
    $('#plan-canvas'+(tabCount-1)).fadeIn(fadeSpeed);

    if (data.comprehensions != null) {
        currentComprehensions[currentExecution] = data.comprehensions;
    }
}

function addToExecutionOrder(currentExecution, newName) {
    if (currentExecution != "") {
        var found = false;
        executionOrder.forEach(function(edge){
            if (edge[0] == currentExecution && edge[1] == newName) {
                found = true;
                return false;
            }
        });
        if (!found)
            executionOrder.push([currentExecution, newName]);
    }
}

function addTab(plan) {
    var shortName = plan.name.substr(0, plan.name.indexOf('$'));

    var executed = 0;
    if (planNames[plan.name])
        executed = planNames[plan.name].executed;

    if (executed > 1)
        shortName += "("+executed+")";

    $('#plan-tabs').append('<li class="tab-title '+((tabCount == 0)?'active':'')+'"><a href="#panel'+(tabCount)+'" plan-name="'+plan.name+'">'+shortName+'</a></li>');
    var content = $('<div class="content '+((tabCount == 0)?'active':'')+'" id="panel'+(tabCount)+'"><canvas id="plan-canvas'+(tabCount)+'" style="display: none;"></canvas></div>');
    content.attr("aria-hidden","true");
    $('#plan-tab-content').append(content);
    tabCount++;
}

function scrollToTab(tab) {
    var planTabs = $('#plan-tabs');
    var middlePoint = planTabs.width() / 2 - tab.width() / 2;
    var scrollPosition = tab.offset().left - tab.parent().offset().left + tab.parent().scrollLeft() - middlePoint;
    planTabs.animate({
        scrollLeft: scrollPosition
    }, 400, "easeOutCirc");
}

function updateTabLabel(planTab, name, planMetaData) {
    planMetaData.executed += 1;
    var shortName = name.substr(0, name.indexOf('$'));

    if (planMetaData.executed > 1)
        planTab.html(shortName+"("+(planMetaData.executed)+")");
}

function setupCodeCanvas() {
    if (codeCanvas == null) {
        codeCanvas = new paper.PaperScope().setup($('#code-canvas')[0]);
    }
}

function drawComprehensionBoxes(name) {
    var boxes = currentComprehensions[name];
    codeCanvas = new paper.PaperScope().setup($('#code-canvas')[0]);

    if (boxes != null && boxes.length > 0) {
        boxes.forEach(function(box){
            drawComprehensionBox(box[0] - codeStartIndex - 1, box[1] - codeStartIndex - 1);
        });
    }

    if (iterationMarker.length > 0) {
        var maxWidth = $('#code-canvas').width();
        for (var i in iterationMarker) {
            var iteration = iterationMarker[i];
            var clientRect = setSelectionRange($('#code-container').find('code')[0], iteration[0] - codeStartIndex - 1, iteration[1] - codeStartIndex - 1);
            if (clientRect != null && codeCanvas) {
                var margin = 3;
                var rectangle = new codeCanvas.Shape.Rectangle(
                    new codeCanvas.Point(margin, clientRect.top - margin),
                    maxWidth - 2*margin,
                    clientRect.height + margin
                );
                rectangle.strokeColor = iterationColors[i%iterationColors.length];
                rectangle.strokeWidth = 2;
            }
        }
    }

    codeCanvas.view.draw();
}

function drawComprehensionBox(begin, end) {
    var margin = 4;
    var clientRect = setSelectionRange($('#code-container').find('code')[0], begin, end);

    if (clientRect != null) {
        var maxWidth = $('#code-canvas').width();
        var rectangle = new codeCanvas.Shape.Rectangle(
            new codeCanvas.Point(margin, clientRect.top - margin),
            maxWidth - 2*margin,
            clientRect.height + margin
        );
        rectangle.fillColor = comprehensionHighlightColor;
    }
}

function updateComprehensionBoxes() {
    updateCodeCanvasSize();
    var comprehensionName = $('#plan-tabs').find('li.active a').attr("plan-name");
    drawComprehensionBoxes(comprehensionName);
}

function setSelectionRange(el, start, end) {
    if (document.createRange && window.getSelection) {
        var range = document.createRange();
        range.selectNodeContents(el);
        var textNodes = getTextNodesIn(el);
        var foundStart = false;
        var charCount = 0, endCharCount;

        for (var i = 0, textNode; textNode = textNodes[i++]; ) {
            var text = $(textNode).text();
            endCharCount = charCount + textNode.length;

            var lineBreakCount = text.split("\n").length - 1;

            endCharCount += lineBreakCount;

            if (!foundStart && start >= charCount && (start < endCharCount || (start == endCharCount && i <= textNodes.length))) {
                //start from previous character if selected character is a newline
                if (text[start - charCount] == '\n')
                    start--;

                range.setStart(textNode, start - charCount - lineBreakCount);
                foundStart = true;
            }

            if (foundStart && end <= endCharCount) {
                range.setEnd(textNode, end - charCount);
                break;
            }

            charCount = endCharCount;
        }

        var codeContainer = $(el).parents('#code-container');
        var offset = codeContainer.offset();
        var clientRect = range.getBoundingClientRect();

        var box = null;
        if (range.getClientRects().length > 0) {
            var box = {
                top: clientRect.top - offset.top + codeContainer.scrollTop() + 1,
                left: range.getClientRects()[0].left - offset.left + codeContainer.scrollLeft(),
                width: clientRect.width,
                height: clientRect.height || 19
            };
        }
        return box;
    } else if (document.selection && document.body.createTextRange) {
        var textRange = document.body.createTextRange();
        textRange.moveToElementText(el);
        textRange.collapse(true);
        textRange.moveEnd("character", end);
        textRange.moveStart("character", start);
        textRange.select();
    }
}

function filterParallelizeFunction(code) {
    codeStartIndex = code.indexOf('emma.parallelize');

    while (code[--codeStartIndex] != '\n') {}

    var bracesCount = 0;
    var start = false;
    var index = codeStartIndex;
    while (!start || bracesCount != 0) {
        if (!start && code[index] == '{')
            start = true;

        if (code[index] == '{')
            bracesCount++;

        if (code[index] == '}')
            bracesCount--;

        index++;
    }

    return code.substr(codeStartIndex + 1, index-codeStartIndex);
}

function getTextNodesIn(node) {
    var textNodes = [];
    if (node.nodeType == 3) {
        textNodes.push(node);
    } else {
        var children = node.childNodes;
        for (var i = 0, len = children.length; i < len; ++i) {
            textNodes.push.apply(textNodes, getTextNodesIn(children[i]));
        }
    }
    return textNodes;
}

function resizeContainers() {
    resizeLeftView();
    resizeRightView();
}

function resizeRightView() {
    var wrapper = $("#tab-wrapper");
    var content = $('#plan-tab-content');
    var height = $("html").height() - wrapper.offset().top - 11;
    var width = content.width();
    wrapper.css("height", height);

    height -= $('#plan-tabs').height();
    content.find('.content').each(function(i, tabContent){
        $(tabContent).css("height", height);
    });

    for(var id in canvases) {
        var canvas = canvases[id];
        canvas.view.viewSize = [width, height];
        canvas.view.update(true);
    }
}

function resizeLeftView() {
    var codeWrapper = $(".code-wrapper");
    codeWrapper.resizable({
        minWidth: codeWrapper.parent().width()*0.2,
        maxWidth: codeWrapper.parent().width()*0.8,
        handles: 'e'
    });

    var wrapper = $("#code-tab-wrapper");

    var codeTabs = $('#code-tabs');

    var maxHeight = $("html").height()
        - wrapper.offset().top
        - codeTabs.height()
        - wrapper.find('.ui-resizable-handle').height()
        - $('#log-options').height()
        - 14;   //margin bottom

    if (codeTabs.find('li').css('borderBottomWidth').replace('px','') == 0) {
        //firefox fix
        maxHeight += 2
    }

    if (wrapper.find('.ui-resizable-handle').height() == null) {
        maxHeight -= 7;
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
    var codeContainer = $("#code-container");

    if (codeContainer.css('paddingBottom').replace('px','') == "0") {
        codeHeight -=3;
    }
    codeContainer.css("height", codeHeight);
    $('#log-container').css("height", logHeight);

    wrapper.resizable({
        minHeight: maxHeight*codeHeightMinFactor,
        maxHeight: maxHeight*codeHeightMaxFactor,
        handles: 's'
    });
}

function updateCodeCanvasSize() {
    var codeContainer = $('#code-container')[0];
    $('#code-canvas').css("width", 1)
        .css("height", 1)
        .css("width", codeContainer.scrollWidth)
        .css("height", codeContainer.scrollHeight);
}

function run(async, callback) {
    var running = $('#play-button').find('i').hasClass("fi-pause");

    if (!running) {
        setRunningState();
        addLogHtml("<div class='log-entry'>running: "+currentExecution+"</div>");
        $.ajax({
            method: "GET",
            url: requestBase+"plan/run",
            async: async,
            success: function(data) {
                if (data.graph != null) {
                    var planIndex = planNames[currentExecution].index;
                    var planTab = $('a[href="#panel'+planIndex+'"]');
                    updateTabLabel(planTab, currentExecution, planNames[currentExecution]);
                    buildPlan(data);

                    detectIterations();
                    markIterations(fullExampleName);

                    planIndex = planNames[currentExecution].index;
                    planTab = $('a[href="#panel'+planIndex+'"]');
                    planTab.click();

                    for(var id in canvases) {
                        canvases[id].view.update(true);
                    }
                }

                if (!data.isLast) {
                    setWaitingState();
                } else {
                    setFinishState();
                }

                if (callback) {
                    callback(data);
                }
            },
            error: handleError
        });
    } else {
        fastForwardRun = false;
        setStoppingState();
    }
}

function fastForward() {
    run(true, function(data){
        if (!data.isLast) {
            if (fastForwardRun)
                fastForward();
        }
    });
    fastForwardRun = true;
}

function detectIterations() {
    var vertices = [];
    for (var name in planNames) {
        vertices[planNames[name].index] = new Vertex(name);
    }

    for (var i in executionOrder) {
        var edge = executionOrder[i];
        var leftIndex = planNames[edge[0]].index;
        var rightIndex = planNames[edge[1]].index;

        vertices[leftIndex].connections.push(vertices[rightIndex]);
    }

    var graph = new TarjanGraph(vertices);
    var tarjan = new Tarjan(graph);

    var iterations = tarjan.run();

    if (iterations.length > 0) {
        var iterationElements = [];
        for(var i in iterations) {
            var iteration = iterations[i];
            var elements = [];
            for (var j in iteration) {
                var plan = iteration[j];
                elements.push(plan.name);
            }
            iterationElements.push(elements);
        }
        localStorage.setItem(fullExampleName+"_iterations", JSON.stringify(iterationElements));
    }
}

function markIterations(name){
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
                if (currentComprehensions[planName]) {
                    if (currentComprehensions[planName][0][0] < minSelection)
                        minSelection = currentComprehensions[planName][0][0];
                    if (currentComprehensions[planName][0][1] > maxSelection)
                        maxSelection = currentComprehensions[planName][0][1];
                }
            }
            iterationMarker[i] = [minSelection, maxSelection];
        }
    } else {
        iterationMarker = [];
    }
}

function addLogHtml(html) {
    $('#log-container').append(html);
    trimLog();
    logScrollBottom();
}

function trimLog() {
    if (checkLogSizeIteration++ % 20 == 0) {
        var logContainer = $('#log-container');
        var lineCount = logContainer.find('div').length;
        if (lineCount > logBufferSize) {
            logContainer.find("div:lt("+(lineCount - logBufferSize)+")").remove();
        }
        checkLogSizeIteration = 1;
    }
}

function setInitState() {
    $("#play-button").html('');
    $("#forward-button").html('');
    $('#status').html('');
}

function setRunningState() {
    $("#play-button").html('<i class="fi-pause"></i>');
    $('#status').html('<i class="gear"/> compiling and executing "'+currentExecution+'"');
}

function setWaitingState() {
    $("#play-button").html('<i class="fi-play"></i>');
    $("#forward-button").html('<i class="fi-fast-forward"></i>');
    $('#status').html('ready');
}

function setFinishState() {
    $("#play-button").html('');
    $("#forward-button").html('');
    $('#status').html("finished");
}

function setStoppingState() {
    $("#play-button").html('<i class="fi-play"></i>');
    $("#forward-button").html('<i class="fi-fast-forward"></i>');
    $('#status').html('<i class="gear"/> stopping...');
}

function logScrollBottom() {
    if (scrollLock) {
        var log = document.getElementById('log-container');
        log.scrollTop = log.scrollHeight;
    }
}

function clearCache() {
    for(var key in localStorage)
    {
        if (key.startsWith("eu.stratosphere")) {
            localStorage.removeItem(key);
        }
    }
    clearPage();
}

function registerListeners() {
    $(window).resize(resizeContainers);

    //log listener
    var eventSource = new EventSource(requestBase+"log");
    eventSource.onmessage = function(event) {
        addLogHtml("<div>"+event.data+"</div>");
    };
    eventSource.onerror = function(e) {
        console.error(e);
        setWaitingState();
        addLogHtml("<div class='log-error'>ERROR: Lost connection to Log server. Reload to try again!</div>");
        alert("An error occurred! Detailed information in the log.");
        eventSource.close();
    };

    //key listener
    window.onkeydown = function(e) {
        var key = e.keyCode ? e.keyCode : e.which;
        if (key == 120) {    //F9
            run();
        }

        if (key == 39) {
            //select to next tab
            $('#plan-tabs').find('li.active').next().find('a').click();
        }

        if (key == 37) {
            //select to previous tab
            $('#plan-tabs').find('li.active').prev().find('a').click();
        }
    };

    //code tab click
    $('#code-tabs').on('toggled', function (event, tab) {
        if (tab.find('a').attr('href') == '#code-panel1') {
            logScrollBottom();
            $(this).find('li a[href="#code-panel1"]').html("Log");
            $('#log-options').show();
            $('#code-options').hide();
        } else {
            $('#log-options').hide();
            $('#code-options').show();
            updateComprehensionBoxes();
        }
    });

    //plan tab click
    $('#plan-tabs').on('toggled', function (event, tab) {
        if (codeCanvas != null) {
            codeCanvas.project.activeLayer.removeChildren();
            codeCanvas.view.draw();
            var planCanvasId = tab.find('a').attr('href').replace('#panel','#plan-canvas');
            $(planCanvasId).hide().fadeIn(fadeSpeed);

            scrollToTab(tab);
        }
        updateComprehensionBoxes();
    });

    //clear log button click
    $('#clear-log-button').click(function(){
        $('#log-container').html("");
    });

    //click scroll lock switch
    $('#scroll-lock').change(function(){
        scrollLock = this.checked;
        if (scrollLock) {
            logScrollBottom();
        }
    });

    //zoom button click
    $('#code-zoom-in').click(function(){
        var codeContainer = $('#code-container');
        var fontSize = parseInt(codeContainer.css('font-size').replace('px',''));
        codeContainer.css('font-size',(fontSize+fontSizeStep)+"px");
        updateComprehensionBoxes();
    });

    //zoom button click
    $('#code-zoom-out').click(function(){
        var codeContainer = $('#code-container');
        var fontSize = parseInt(codeContainer.css('font-size').replace('px',''));
        codeContainer.css('font-size',(fontSize-fontSizeStep)+"px");
        updateComprehensionBoxes();
    });
}