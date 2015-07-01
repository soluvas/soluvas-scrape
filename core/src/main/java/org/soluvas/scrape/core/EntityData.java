package org.soluvas.scrape.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ceefour on 7/1/15.
 */
public class EntityData implements Serializable {
    private String id;
    private String name;
    private List<PropertyData> properties = new ArrayList<>();

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setProperties(List<PropertyData> properties) {
        this.properties = properties;
    }

    public List<PropertyData> getProperties() {
        return properties;
    }
}
