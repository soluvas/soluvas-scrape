package org.soluvas.scrape.core;

import com.google.common.collect.ImmutableMap;
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
public class FetcherTest {

    private static final Logger log = LoggerFactory.getLogger(FetcherTest.class);

    @Inject
    private Environment env;
    @Inject
    private ScrapeTemplateRepositoryImpl scrapeTemplateRepo;
    @Inject
    private Fetcher fetcher;
    private ScrapeTemplate template;

    @Configuration
    @ComponentScan(basePackageClasses = Fetcher.class)
    public static class Config {}

    @Before
    public void setUp() {
        template = scrapeTemplateRepo.add(new File(
                "sample/ppdb/school_select.ScrapeTemplate.jsonld"
        ));
    }

    @Test
    public void fetch() {
        fetcher.fetch(template,
                ImmutableMap.of("level", "senior"));
    }

}
