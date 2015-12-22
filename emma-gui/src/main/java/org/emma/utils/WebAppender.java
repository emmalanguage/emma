package org.emma.utils;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

/**
 * Created by Andi on 04.11.2015.
 */
public class WebAppender extends AppenderSkeleton {
    @Override
    protected void append(LoggingEvent event) {
        LogQueue.getInstance().addMessage((String) event.getMessage());
    }

    @Override
    public void close() {

    }

    @Override
    public boolean requiresLayout() {
        return false;
    }
}
