package org.soluvas.scrape.core;

/**
 * Created by ceefour on 7/1/15.
 */
public class ScrapeException extends RuntimeException {

    public ScrapeException() {
    }

    public ScrapeException(String message) {
        super(message);
    }

    public ScrapeException(String message, Throwable cause) {
        super(message, cause);
    }

    public ScrapeException(Throwable cause) {
        super(cause);
    }

    public ScrapeException(Throwable cause, String format, Object... args) {
        super(String.format(format, args), cause);
    }

    public ScrapeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
