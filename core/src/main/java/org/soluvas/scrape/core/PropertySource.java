package org.soluvas.scrape.core;

/**
 * Created by ceefour on 7/2/15.
 */
public enum PropertySource {
    /**
     * Content of the property is from scraped response.
     */
    RESPONSE,
    /**
     * From a request/RPC parameter.
     */
    REQUEST_PARAMETER
}
