package org.soluvas.scrape.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ceefour on 7/1/15.
 */
public class CollectionDef implements Serializable {
    private String id;
    private String name;
    private MappingSource source;
    private String sourceExpression;
    private String idProperty;
    private String nameProperty;
    /**
     * @see org.soluvas.data.PropertyDefinition
     */
    private List<PropertyDef> properties = new ArrayList<>();

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

    public MappingSource getSource() {
        return source;
    }

    public void setSource(MappingSource source) {
        this.source = source;
    }

    public String getSourceExpression() {
        return sourceExpression;
    }

    public void setSourceExpression(String sourceExpression) {
        this.sourceExpression = sourceExpression;
    }

    public String getIdProperty() {
        return idProperty;
    }

    public void setIdProperty(String idProperty) {
        this.idProperty = idProperty;
    }

    public String getNameProperty() {
        return nameProperty;
    }

    public void setNameProperty(String nameProperty) {
        this.nameProperty = nameProperty;
    }

    public List<PropertyDef> getProperties() {
        return properties;
    }

    public void setProperties(List<PropertyDef> properties) {
        this.properties = properties;
    }
}
