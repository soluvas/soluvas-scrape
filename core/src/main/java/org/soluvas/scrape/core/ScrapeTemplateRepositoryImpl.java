package org.soluvas.scrape.core;

import org.soluvas.json.JsonUtils;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Repository;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ceefour on 7/1/15.
 */
@Repository
@Profile({"scraper", "sql"})
public class ScrapeTemplateRepositoryImpl {
    final Map<String, ScrapeTemplate> templates = new HashMap<>();

    public Map<String, ScrapeTemplate> getTemplates() {
        return templates;
    }

    public ScrapeTemplate add(File file) {
        try {
            final ScrapeTemplate template = JsonUtils.mapper.readValue(file, ScrapeTemplate.class);
            templates.put(template.getId(), template);
            return template;
        } catch (IOException e) {
            throw new ScrapeException(e, "Cannot load ScrapeTemplate '%s'", file);
        }
    }
}
