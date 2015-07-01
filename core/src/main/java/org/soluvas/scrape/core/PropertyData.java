package org.soluvas.scrape.core;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ceefour on 7/1/15.
 */
public class PropertyData {
    @JsonIgnore
    private PropertyDef definition;
    private List<PropertyValue> values = new ArrayList<>();

    public PropertyDef getDefinition() {
        return definition;
    }

    public void setDefinition(PropertyDef definition) {
        this.definition = definition;
    }

    public List<PropertyValue> getValues() {
        return values;
    }

}
