package org.soluvas.scrape.core;

/**
 * Created by ceefour on 7/1/15.
 */
public enum MappingSource {
    /**
     * Root response.
     */
    ROOT,
    /**
     * Path based from root response.
     * For JSON-RPC, this uses <a href="http://goessner.net/articles/JsonPath/">JSONPath</a>.
     */
    PATH
}
