package org.soluvas.scrape.core.jsonrpc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.soluvas.scrape.core.ScrapeException;

/**
 * Created by ceefour on 7/2/15.
 */
public class JsonRpcException extends ScrapeException {

    @JsonCreator
    public JsonRpcException(@JsonProperty("code") int code, @JsonProperty("msg") String message) {
        super(code + ": " + message);
    }

}
