var Logger = function(elementId){
    var jqueryElement = $("#"+elementId);
    var logger = {
        connected: false,
        scrollLock: true,
        logBufferSize: 1000,
        ringbuffer: [],
        bufferIndex: 0,
        lastUpdateIndex: null,
        checkLogSizeIteration: 0,
        reconnectInterval: null,

        start: function () {
            if (!this.connected) {
                var self = this;

                setInterval(function(){
                    if (self.lastUpdateIndex != self.bufferIndex) {
                        self.update();
                        self.logScrollBottom();
                        self.lastUpdateIndex = self.bufferIndex;
                    }
                }, 1000);

                this.createEventSource();

                //clear log button click
                $('#clear-log-button').click(function(){
                    self.clear();
                });

                //click scroll lock switch
                $('#scroll-lock').change(function(){
                    self.scrollLock = this.checked;
                    if (this.checked) {
                        self.logScrollBottom();
                    }
                });

                this.connected = true;
            } else {
                console.warn("Log already connected");
            }
        },

        createEventSource: function() {
            var self = this;
            var eventSource = new EventSource(requestBase + "log");


            eventSource.onmessage = function (event) {
                self.log("<div>" + event.data + "</div>");
            };

            eventSource.onerror = function(e) {
                eventSource.close();
                console.warn(e);
                self.reconnectInterval = setInterval(function() {self.reconnect()}, 500);
            };
        },

        update: function() {
            var lastIndex = this.bufferIndex % this.logBufferSize;
            var currentIndex = this.bufferIndex % this.logBufferSize;
            jqueryElement.html("");
            do {
                if (this.ringbuffer[currentIndex]) {
                    jqueryElement.append(this.ringbuffer[currentIndex]);
                }
                currentIndex = (currentIndex+1) % this.logBufferSize;
            } while (currentIndex != lastIndex)
        },

        log: function (html) {
            this.ringbuffer[this.bufferIndex++%this.logBufferSize] = html;
        },

        logScrollBottom: function() {
            if (this.scrollLock) {
                var log = document.getElementById('log-container');
                log.scrollTop = log.scrollHeight;
            }
        },

        clear: function() {
            this.ringbuffer = [];
            this.bufferIndex = 0;
            this.lastUpdateIndex = null;
            jqueryElement.html("");
        },

        reconnect: function() {
            console.log("reconnecting to log server...");
            this.createEventSource();
            if (this.reconnectInterval != null) {
                clearInterval(this.reconnectInterval);
            }
        }
    };
    return logger;
};