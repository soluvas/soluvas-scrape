package org.soluvas.scrape.core;

import java.io.Serializable;

/**
 * Created by ceefour on 7/1/15.
 */
public class PropertyDef implements Serializable {
    private String id;
    private PropertyKind kind;
    private String enumerationId;
    private Cardinality cardinality;
    private PropertySource source;
    private String parameterId;

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

    public String getEnumerationId() {
        return enumerationId;
    }

    public void setEnumerationId(String enumerationId) {
        this.enumerationId = enumerationId;
    }

    public Cardinality getCardinality() {
        return cardinality;
    }

    public void setCardinality(Cardinality cardinality) {
        this.cardinality = cardinality;
    }

    public PropertySource getSource() {
        return source;
    }

    public void setSource(PropertySource source) {
        this.source = source;
    }

    /**
     * If {@link #getSource()} is {@link PropertySource#REQUEST_PARAMETER},
     * this sets which {@link RpcParam} is used as the value source.
     * @return
     */
    public String getParameterId() {
        return parameterId;
    }

    public void setParameterId(String parameterId) {
        this.parameterId = parameterId;
    }
}
