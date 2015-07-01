package org.soluvas.scrape.core;

/**
 * Created by ceefour on 7/1/15.
 */
public enum Cardinality {
    /**
     * 0 or 1.
     */
    SINGLE,
    /**
     * 0 or more. It will use a {@link java.util.List} or {@link java.util.Set}.
     */
    MULTIPLE
}
