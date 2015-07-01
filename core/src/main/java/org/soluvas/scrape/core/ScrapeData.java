package org.soluvas.scrape.core;

import org.joda.time.DateTime;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by ceefour on 7/1/15.
 */
public class ScrapeData implements Serializable {
    private DateTime creationTime;
    private List<CollectionData> collections = new ArrayList<>();

    public DateTime getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(DateTime creationTime) {
        this.creationTime = creationTime;
    }

    public List<CollectionData> getCollections() {
        return collections;
    }

}
