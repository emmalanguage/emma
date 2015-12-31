package org.emma.utils;

import java.util.concurrent.ConcurrentLinkedQueue;

public class LogQueue {

    private static LogQueue instance = null;
    private static ConcurrentLinkedQueue<String> logQueue = null;

    private LogQueue() {
        logQueue = new ConcurrentLinkedQueue<>();
    }

    public static LogQueue getInstance() {
        if (instance == null)
            instance = new LogQueue();

        return instance;
    }

    public void addMessage(String message) {
        logQueue.add(message);
    }

    public String getNextMessage() {
        return logQueue.poll();
    }
}
