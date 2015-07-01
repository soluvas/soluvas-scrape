package org.soluvas.scrape.core;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by ceefour on 7/2/15.
 */
public class FetchData {

    private String uri;
    private ScrapeTemplate.Protocol protocol;
    private String protocolVersion;
    private Map<String, Object> requestParams = new LinkedHashMap<>();
    private JsonRpc2MethodResult jsonRpcResult;

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public ScrapeTemplate.Protocol getProtocol() {
        return protocol;
    }

    public void setProtocol(ScrapeTemplate.Protocol protocol) {
        this.protocol = protocol;
    }

    public String getProtocolVersion() {
        return protocolVersion;
    }

    public void setProtocolVersion(String protocolVersion) {
        this.protocolVersion = protocolVersion;
    }

    public Map<String, Object> getRequestParams() {
        return requestParams;
    }

    public JsonRpc2MethodResult getJsonRpcResult() {
        return jsonRpcResult;
    }

    public void setJsonRpcResult(JsonRpc2MethodResult jsonRpcResult) {
        this.jsonRpcResult = jsonRpcResult;
    }
}
