package org.emma.servlets;

import org.eclipse.jetty.servlets.EventSource;
import org.eclipse.jetty.servlets.EventSourceServlet;
import org.emma.utils.LogQueue;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

public class LogEventServlet extends EventSourceServlet {

    @Override
    protected EventSource newEventSource(final HttpServletRequest req) {
        return new EventSource() {

            @Override
            public void onOpen(final Emitter emitter) throws IOException {
                while (true) {
                    String message;
                    while ((message = LogQueue.getInstance().getNextMessage()) != null) {
                        emitter.data(message + "\n");
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            @Override
            public void onClose() {}
        };
    }
}