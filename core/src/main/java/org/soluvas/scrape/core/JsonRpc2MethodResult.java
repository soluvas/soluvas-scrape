package org.soluvas.scrape.core;

import com.fasterxml.jackson.databind.JsonNode;
import org.soluvas.scrape.core.jsonrpc.JsonRpcException;

/**
 * Note: {@link JsonNode} is not {@link java.io.Serializable}.
 * Created by ceefour on 7/1/15.
 */
public class JsonRpc2MethodResult {
    private String jsonrpc;
    private JsonNode result;
    private String id;
    private JsonRpcException error;

    public String getJsonrpc() {
        return jsonrpc;
    }

    public void setJsonrpc(String jsonrpc) {
        this.jsonrpc = jsonrpc;
    }

    public JsonNode getResult() {
        return result;
    }

    public void setResult(JsonNode result) {
        this.result = result;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public JsonRpcException getError() {
        return error;
    }

    public void setError(JsonRpcException error) {
        this.error = error;
    }

    @Override
    public String toString() {
        return "JsonRpc2MethodResult{" +
                "jsonrpc='" + jsonrpc + '\'' +
                ", result=" + result +
                ", id='" + id + '\'' +
                ", error=" + error +
                '}';
    }
}
