package org.soluvas.scrape.core;

import org.hibernate.annotations.Type;
import org.joda.time.DateTime;

import javax.persistence.*;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * A Task which performs 3 related subtasks sequentially:
 * <ol>
 *     <li>{@link Fetcher#fetch(ScrapeTemplate, Map)}</li>
 *     <li>{@link Scraper#scrape(ScrapeTemplate, FetchData)}</li>
 *     <li>{@link Scraper#scrape(ScrapeTemplate, FetchData)}</li>
 * </ol>
 * Created by ceefour on 7/2/15.
 */
@Entity
@Table(schema = "scrape")
public class FetchToUpsertTask implements Serializable {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    @Column(columnDefinition = "timestamp with time zone", nullable = false)
    @Type(type = "org.jadira.usertype.dateandtime.joda.PersistentDateTime")
    private DateTime creationTime;
    @Column(nullable = false)
    private String scrapeTemplateId;
    @Type(type = "org.soluvas.scrape.core.sql.PersistentMap")
    @Column(columnDefinition = "hstore", nullable = false)
    private Map<String, Object> requestParams = new LinkedHashMap<>();

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public DateTime getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(DateTime creationTime) {
        this.creationTime = creationTime;
    }

    public String getScrapeTemplateId() {
        return scrapeTemplateId;
    }

    public void setScrapeTemplateId(String scrapeTemplateId) {
        this.scrapeTemplateId = scrapeTemplateId;
    }

    public Map<String, Object> getRequestParams() {
        return requestParams;
    }

}
