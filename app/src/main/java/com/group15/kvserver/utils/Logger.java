package com.group15.kvserver.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * The Logger class provides a utility for logging messages with different levels of severity.
 * It formats log messages with a timestamp, a log level, and the message itself.
 */
public class Logger {

    /**
     * Enum representing the different levels of log severity.
     */
    public enum LogLevel {
        INFO, WARN, ERROR, DEBUG
    }

    /**
     * Logs a message to the console with a specified log level.
     *
     * @param message the message to log
     * @param level   the severity level of the log (INFO, WARN, ERROR, DEBUG)
     */
    public static void log(String message, LogLevel level) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String timestamp = sdf.format(new Date());

        System.out.println(String.format("[%s] [%s] %s", timestamp, level, message));
    }
}
