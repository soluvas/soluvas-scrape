package org.soluvas.scrape.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ceefour on 7/1/15.
 */
public class CollectionData implements Serializable {
    private CollectionDef definition;
    private List<EntityData> entities = new ArrayList<>();

    public CollectionDef getDefinition() {
        return definition;
    }

    public void setDefinition(CollectionDef definition) {
        this.definition = definition;
    }

    public List<EntityData> getEntities() {
        return entities;
    }
}
