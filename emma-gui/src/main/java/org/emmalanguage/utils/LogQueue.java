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
package org.emmalanguage.utils;

import java.util.concurrent.ConcurrentLinkedQueue;

public class LogQueue {

    private static LogQueue instance = null;
    private static ConcurrentLinkedQueue<String> logQueue = null;

    private LogQueue() {
        logQueue = new ConcurrentLinkedQueue<>();
    }

    static LogQueue getInstance() {
        if (instance == null)
            instance = new LogQueue();

        return instance;
    }

    void addMessage(String message) {
        logQueue.add(message);
    }

    public String getNextMessage() {
        return logQueue.poll();
    }
}