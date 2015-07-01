package org.soluvas.scrape.core;

import java.io.Serializable;

/**
 * Created by ceefour on 7/1/15.
 */
public class EnumOption implements Serializable {
    private String id;
    private String name;

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
}
