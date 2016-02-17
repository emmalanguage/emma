var Logger = function(elementId){
    var jqueryElement = $("#"+elementId);
    var logger = {
        connected: false,
        scrollLock: true,
        logBufferSize: 500,
        ringbuffer: [],
        bufferIndex: 0,
        lastUpdateIndex: null,
        checkLogSizeIteration: 0,
        start: function () {
            if (!this.connected) {
                var self = this;

                var updateInterval = setInterval(function(){
                    if (self.lastUpdateIndex != self.bufferIndex) {
                        self.update();
                        self.logScrollBottom();
                        self.lastUpdateIndex = self.bufferIndex;
                    }
                }, 1000);

                var eventSource = new EventSource(requestBase + "log");

                eventSource.onmessage = function (event) {
                    self.log("<div>" + event.data + "</div>");
                };

                eventSource.onerror = function(e) {
                    self.log("<div class='log-error'>ERROR: Lost connection to Log server. Reload to try again!</div>");
                    alert("An error occurred! Detailed information in the log.");
                    console.error(e);
                    eventSource.close();
                };

                //clear log button click
                $('#clear-log-button').click(function(){
                    jqueryElement.html("");
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

        update: function() {
            var lastIndex = this.bufferIndex % this.logBufferSize;
            var currentIndex = this.bufferIndex % this.logBufferSize;
            this.clear();
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
            jqueryElement.html("");
        }
    };
    return logger;
};