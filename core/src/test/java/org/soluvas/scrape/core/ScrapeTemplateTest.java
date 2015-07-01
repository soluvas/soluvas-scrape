package org.soluvas.scrape.core;

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
public class ScrapeTemplateTest {

    private static final Logger log = LoggerFactory.getLogger(ScrapeTemplateTest.class);

    @Inject
    private Environment env;
    @Inject
    private ScrapeTemplateRepositoryImpl scrapeTemplateRepo;

    @Configuration
    @ComponentScan(basePackageClasses = Fetcher.class)
    public static class Config {}

    @Before
    public void setUp() {
    }

    @Test
    public void add() {
        final ScrapeTemplate template = scrapeTemplateRepo.add(new File(
                "sample/ppdb/school_select.ScrapeTemplate.jsonld"
        ));
        log.info("ScrapeTemplate: {}", template);
        Assert.assertEquals("ppdb", template.getId());
        Assert.assertThat(template.getRpcParams(),
                Matchers.hasSize(1));
        Assert.assertThat(template.getEnumerations(),
                Matchers.hasSize(1));
        Assert.assertThat(template.getCollections(),
                Matchers.hasSize(1));
        final CollectionDef coll = template.getCollections().get(0);
        Assert.assertNotNull(coll);
        Assert.assertEquals("school", coll.getId());
        Assert.assertThat(coll.getProperties(),
                Matchers.hasSize(Matchers.greaterThan(5)));
    }

}
