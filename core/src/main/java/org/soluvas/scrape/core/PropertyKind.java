package org.soluvas.scrape.core;

/**
 * Created by ceefour on 7/1/15.
 */
public enum PropertyKind {
    ENUMERATION,
    INTEGER,
    /**
     * UTF-8 text, 255 chars max.
     */
    STRING,
    /**
     * Long UTF-8 text.
     */
    TEXT,
    BOOLEAN,
    DOUBLE,
    LOCAL_DATE,
    DATE_TIME,
    /**
     * Arbitrary {@link com.fasterxml.jackson.databind.node.ObjectNode}.
     */
    JSON_OBJECT
}
