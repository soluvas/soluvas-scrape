package org.soluvas.scrape.core;

import com.google.common.collect.ImmutableMap;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.ConfigFileApplicationContextInitializer;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.inject.Inject;
import java.io.File;

/**
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(initializers = ConfigFileApplicationContextInitializer.class)
@ActiveProfiles("scraper")
public class ScraperTest {

    private static final Logger log = LoggerFactory.getLogger(ScraperTest.class);

    @Inject
    private Environment env;
    @Inject
    private ScrapeTemplateRepositoryImpl scrapeTemplateRepo;
    @Inject
    private Fetcher fetcher;
    @Inject
    private Scraper scraper;
    private ScrapeTemplate schoolSelect;
    private ScrapeTemplate registeredSelect;

    @Configuration
    @ComponentScan(basePackageClasses = Fetcher.class)
    public static class Config {}

    @Before
    public void setUp() {
        schoolSelect = scrapeTemplateRepo.add(new File(
                "sample/ppdb/school_select.ScrapeTemplate.jsonld"
        ));
        registeredSelect = scrapeTemplateRepo.add(new File(
                "sample/ppdb/registered_select.ScrapeTemplate.jsonld"
        ));
    }

    @Test
    public void scrapeSchool() {
        final FetchData result = fetcher.fetch(schoolSelect,
                ImmutableMap.of("level", "senior"));
        final ScrapeData scrapeData = scraper.scrape(schoolSelect, result);
        log.info("Scrape data: {}", scrapeData);
        Assert.assertThat(scrapeData.getCollections(), Matchers.hasSize(1));
        final CollectionData coll = scrapeData.getCollections().get(0);
        Assert.assertNotNull(coll);
        Assert.assertThat(coll.getEntities(), Matchers.hasSize(Matchers.greaterThan(5)));
        final EntityData entity1 = coll.getEntities().get(0);
        Assert.assertThat(entity1.getProperties(), Matchers.hasSize(Matchers.greaterThan(5)));
        final PropertyData entity1Level = entity1.getProperties().stream().filter(prop -> prop.getDefinition().getId().equals("level")).findAny().get();
        Assert.assertEquals("senior", entity1Level.getValues().get(0).getString());
    }

    @Test
    public void scrapeRegistered() {
        final FetchData result = fetcher.fetch(registeredSelect,
                ImmutableMap.of("choice_id", 726));
        final ScrapeData scrapeData = scraper.scrape(registeredSelect, result);
        log.info("Scrape data: {}", scrapeData);
        Assert.assertThat(scrapeData.getCollections(), Matchers.hasSize(1));
        final CollectionData coll = scrapeData.getCollections().get(0);
        Assert.assertNotNull(coll);
        Assert.assertThat(coll.getEntities(), Matchers.hasSize(Matchers.greaterThan(5)));
        final EntityData entity1 = coll.getEntities().get(0);
        Assert.assertThat(entity1.getProperties(), Matchers.hasSize(Matchers.greaterThan(5)));
    }

}
