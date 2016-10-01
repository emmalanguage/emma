/**
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