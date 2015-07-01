package org.soluvas.scrape.core;

import java.io.Serializable;
import java.util.List;

/**
 * Used by {@link org.soluvas.scrape.core.ScrapeTemplate.Protocol#JSONRPC}.
 * Created by ceefour on 7/1/15.
 */
public class RpcParam implements Serializable {
    private String id;
    private PropertyKind kind;
    private String enumerationId;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public PropertyKind getKind() {
        return kind;
    }

    public void setKind(PropertyKind kind) {
        this.kind = kind;
    }

    /**
     * For {@link PropertyKind#ENUMERATION}: enumeration options.
     * @return
     */
    public String getEnumerationId() {
        return enumerationId;
    }

    public void setEnumerationId(String enumerationId) {
        this.enumerationId = enumerationId;
    }

}
