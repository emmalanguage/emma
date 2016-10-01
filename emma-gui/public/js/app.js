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
'use strict';

var logger = new Logger("log-container");
var planView = new PlanView();
var codeView = new CodeView();
var scrollCode = true;
var requestBase = "http://localhost:8080/";
var fullExampleName = "";
var planNames = {};
var planCache = [];
var currentExecution = "";
var currentState = 'init';
var executionOrder = [];
var loadedExample = "";
var planCaching = true;
var fontSizeStep = 2;
var codeHeightMinFactor = 0.2;
var codeHeightMaxFactor = 0.9;
var fastForwardRun = true;
var iterationColors = [
    '#24E0FB',
    '#DFDF00',
    '#3923D6',
    '#BBBBFF',
    '#01F33E',
    '#80B584'
];

logger.start();
registerListeners();

function clearPage(){
    var code = $('#code-container').find('code');
    code.html("");
    code.width("auto");
    code.parent().width("auto");

    init();

    $('#code-canvas').css("height", "0px");
}

function init() {
    planView.init();
    planNames = {};
    planCache = [];
    currentExecution = "";
    executionOrder = [];
    loadedExample = "";

    setInitState();
    $('#example-name').find('div').html("");
    $('#submit-parameters').hide();
    logger.clear();

    codeView.init();
}

var handleError = function(jqXHR, textStatus, errorThrown) {
    console.error(textStatus, errorThrown, jqXHR);
    setReadyState();
    logger.log("<div class='log-error'>ERROR: status: '"+textStatus+"', errorThrown: '"+errorThrown+"'</div>");
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
    updateExampleName(cache);

    $('#status').html('<i class="gear"/> loading plan...');

    loadCode(name);

    if (planCaching && cache != null) {
        loadPlanFromCache(name, cache);
    } else {
        loadPlan(name);
    }

    highlightCurrentExample(name);
}

function updateExampleName(cache) {
    var name = fullExampleName;

    var exampleName = $("a[title='" + name + "']").text();
    if (exampleName) {
        var runtime = $('#runtime').val();
        $('#example-name').find('div').html("<span>" + runtime + ":</span> " + exampleName + ((planCaching && cache != null) ? ' (cached)' : ''));
    } else {
        $('#example-name').find('div').html("");
    }
}

function loadCode(name) {
    codeView.setupCodeCanvas();
    $.ajax({
        method: "GET",
        url: requestBase+"code/"+name,
        success: function(data) {
            if (data != null) {
                codeView.setCodeHasCarriageReturns(data.code.indexOf("\r") > -1);
                codeView.renderCode(data.code);
                if (data.parameters) {
                    codeView.buildParameterForm(data.parameters.attrs_);
                }
            } else {
                console.error("No sources available. Did you compile the sources of emma-examples?");
                code.html("No sources available.");
            }
        },
        error: handleError
    });
}

function loadPlan(name) {
    var runtime = $('#runtime').val();
    $.ajax({
        method: "GET",
        url: requestBase+"plan/loadGraph?name="+name+"&runtime="+runtime,
        success: function(data) {
            logger.log("<div>Init Runtime with default parameters</div>");
            if (data.graph != null) {
                loadedExample = name;
                $('#plan-tab-content').html("");
                planView.buildPlan(data);

                if (data.comprehensions != null) {
                    codeView.currentComprehensions[currentExecution] = data.comprehensions;
                    codeView.updateComprehensionBoxes();
                }

                $('#plan-tabs').find('li:last a').click();

                if (!data.isLast) {
                    setReadyState();
                }

            } else {
                console.error("Requested plan data is null! Request: "+this.url);
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
        planView.buildPlan(plan);

        if (plan.comprehensions != null) {
            codeView.currentComprehensions[currentExecution] = plan.comprehensions;
        }
    });

    planView.markIterations(name, codeView);
    planView.updateCanvases();
    initRuntime(name);

    currentExecution = planCache[0].graph.name;
    planNames[currentExecution].executed = 0;
    setReadyState();
    $('a[href="#panel0"]').click();
}

function initRuntime(name, parameters) {
    executionOrder = [];
    var runtime = $('#runtime').val();
    $.ajax({
        method: "POST",
        url: requestBase+"plan/initRuntime?name="+name+"&runtime="+runtime,
        data: JSON.stringify(parameters),
        contentType:"application/json; charset=utf-8",
        success: function() {
            if (!parameters) {
                logger.log("<div>Init Runtime with default parameters</div>");
            } else {
                var message = "Init Runtime with following parameters:";
                for( var i in parameters) {
                    message += "<br>"+i+": "+parameters[i];
                }
                logger.log("<div>"+message+"</div>");
            }
        },
        error: handleError
    });
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

function resizeContainers() {
    resizeExampleName();
    codeView.resizeView();
    planView.resizeView();
}

function resizeExampleName() {
    var nav = $('nav');
    var width = nav.width();

    nav.children().each(function(i,e){
        var elem = $(e);
        if (elem.attr('id') != 'example-name') {
            width -= elem.width();
        }
    });

    $('#example-name').width(width);
}

function run(async, callback) {

    if (currentState == 'finish') {
        prepareRerun();
        return;
    }

    if (currentState == 'ready') {
        setRunningState();
        logger.log("<div class='log-entry'>running: "+currentExecution+"</div>");
        $.ajax({
            method: "GET",
            url: requestBase+"plan/run",
            async: async,
            success: function(data) {
                if (data.graph != null) {
                    var planName = planNames[currentExecution];
                    var planIndex = planName.index;
                    planName.executed += 1;
                    var planTab = $('a[href="#panel'+planIndex+'"]');

                    planView.updateTabLabel(planTab, currentExecution, planName);
                    planView.buildPlan(data);

                    if (data.comprehensions != null) {
                        codeView.currentComprehensions[currentExecution] = data.comprehensions;
                        codeView.updateComprehensionBoxes();
                    }

                    detectIterations();
                    planView.markIterations(fullExampleName, codeView);

                    planName = planNames[currentExecution];
                    planIndex = planName.index;
                    planTab = $('a[href="#panel'+planIndex+'"]');
                    planTab.click();

                    planView.updateCanvases();
                }

                if (!data.isLast) {
                    setReadyState();
                } else {
                    setFinishState();
                }

                if (callback) {
                    callback(data);
                }
            },
            error: handleError
        });
    } else if (currentState == 'running'){
        fastForwardRun = false;
        setStoppingState();
    }
}

function parseParameters() {
    var parameters = {};
    var parameterElements = $('#parameters').find("input");
    parameterElements.each(function(i, e){
        var element = $(e);
        parameters[element.attr("name")] = element.val();
    });
    return parameters;
}

function prepareRerun(parameters) {
    initRuntime(fullExampleName, parameters);
    currentExecution = planCache[0].graph.name;
    $('a[href="#panel0"]').click();
    for (var name in planNames) {
        planNames[name].executed = 0;
        var planIndex = planNames[name].index;
        var planTab = $('a[href="#panel'+planIndex+'"]');
        planView.updateTabLabel(planTab, name, planNames[name]);
    }

    setReadyState();
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

function setInitState() {
    currentState = 'init';
    $("#play-button").html('');
    $("#forward-button").html('');
    $('#status').html('');
}

function setRunningState() {
    currentState = 'running';
    $("#play-button").html('<i class="fi-pause"></i>');
    $('#status').html('<i class="gear"/> compiling and executing "'+currentExecution+'"');
}

function setReadyState() {
    currentState = 'ready';
    $("#play-button").html('<i class="fi-play"></i>');
    $("#forward-button").html('<i class="fi-fast-forward"></i>');
    $('#status').html('ready');
}

function setFinishState() {
    currentState = 'finish';
    $("#play-button").html('<i class="fi-refresh"></i>');
    $("#forward-button").html('');
    $('#status').html("finished");
}

function setStoppingState() {
    currentState = 'stopping';
    $("#play-button").html('<i class="fi-play"></i>');
    $("#forward-button").html('<i class="fi-fast-forward"></i>');
    $('#status').html('<i class="gear"/> stopping...');
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

function highlightCurrentExample(name) {
    var exampleList = $('#exampleList');
    exampleList.find("li.active").removeClass("active");
    exampleList.find("a[title='"+name+"']").parent().addClass("active");
}

function registerListeners() {
    $(window).resize(resizeContainers);

    //key listener
    window.onkeydown = function(e) {
        var key = e.keyCode ? e.keyCode : e.which;
        if (key == 120) {    //F9
            run();
        }

        planView.keyEvent(key);
    };

    codeView.registerEvents();

    scrollCode = localStorage.getItem("scrollCode") == "true";

    $('#scroll-code').prop("checked", scrollCode);

    $('#runtime').change(function(e){
        var cache = localStorage.getItem(fullExampleName);
        updateExampleName(cache);
        if (planCache.length > 0) {
            prepareRerun(parseParameters());
        }
        localStorage.setItem("runtime",$(e.target).val());
    });

    var runtime = localStorage.getItem("runtime");
    if (runtime != null) {
        $('#runtime').find('option').each(function(i,e){
            var elem = $(e);
            if (elem.text() == runtime) {
                elem.attr("selected","selected");
            }
        })
    }
}