package org.soluvas.scrape.core;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ceefour on 7/1/15.
 */
@JsonTypeInfo(use=com.fasterxml.jackson.annotation.JsonTypeInfo.Id.NAME, property="@type")
@JsonSubTypes(@com.fasterxml.jackson.annotation.JsonSubTypes.Type(name="ScrapeTemplate", value=ScrapeTemplate.class))
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class ScrapeTemplate implements Serializable {

    public enum Protocol {
        /**
         * JSON-RPC, usually v2.0.
         */
        JSONRPC,
        /**
         * HTTP(S), usually v1.1.
         */
        HTTP
    }
    private String id;
    private Protocol protocol;
    private String protocolVersion;
    private String uri;
    private String username;
    private String password;
    private String rpcMethod;
    private List<RpcParam> rpcParams = new ArrayList<>();
    private List<EnumerationDef> enumerations = new ArrayList<>();
    private List<CollectionDef> collections = new ArrayList<>();

    public String getId() {
        return id;
    }

    public Protocol getProtocol() {
        return protocol;
    }

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    public String getProtocolVersion() {
        return protocolVersion;
    }

    public void setProtocolVersion(String protocolVersion) {
        this.protocolVersion = protocolVersion;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * Used by {@link org.soluvas.scrape.core.ScrapeTemplate.Protocol#JSONRPC}.
     * @return
     */
    public String getRpcMethod() {
        return rpcMethod;
    }

    public void setRpcMethod(String rpcMethod) {
        this.rpcMethod = rpcMethod;
    }

    /**
     * Used by {@link org.soluvas.scrape.core.ScrapeTemplate.Protocol#JSONRPC}.
     * @return
     */
    public List<RpcParam> getRpcParams() {
        return rpcParams;
    }

    public void setRpcParams(List<RpcParam> rpcParams) {
        this.rpcParams = rpcParams;
    }

    public List<EnumerationDef> getEnumerations() {
        return enumerations;
    }

    public List<CollectionDef> getCollections() {
        return collections;
    }

}
