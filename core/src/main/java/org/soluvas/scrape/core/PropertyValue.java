package org.soluvas.scrape.core;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.Serializable;

/**
 * Created by ceefour on 7/1/15.
 * @todo Translatable/Localizable property values?
 */
public class PropertyValue implements Serializable {
    private Object value;
    private String name;

    public PropertyValue() {
    }

    public PropertyValue(Object value) {
        this.value = value;
    }

    public PropertyValue(Object value, String name) {
        this.value = value;
        this.name = name;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public String getString() {
        return (String) value;
    }

    public Boolean getBoolean() {
        return (Boolean) value;
    }

    public Integer getInteger() {
        return (Integer) value;
    }

    public ObjectNode getJsonObject() {
        return (ObjectNode) value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
