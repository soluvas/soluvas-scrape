package org.soluvas.scrape.core;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by ceefour on 7/1/15.
 */
public class JsonRpc2MethodCall implements Serializable {
    private String jsonrpc = "2.0"; // fixed
    private String method;
    private Map<String, Object> params = new LinkedHashMap<>();

    public String getJsonrpc() {
        return jsonrpc;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public Map<String, Object> getParams() {
        return params;
    }

}
