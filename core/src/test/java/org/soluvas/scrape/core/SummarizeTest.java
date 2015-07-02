package org.soluvas.scrape.core;

import com.google.common.collect.ImmutableMap;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.postgresql.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.soluvas.scrape.core.sql.TableDmlGenerator;
import org.springframework.boot.test.ConfigFileApplicationContextInitializer;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.inject.Inject;
import java.beans.PropertyVetoException;
import java.io.File;
import java.io.IOException;

/**
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(initializers = ConfigFileApplicationContextInitializer.class)
@ActiveProfiles({"sql", "summarizer"})
public class SummarizeTest {

    private static final Logger log = LoggerFactory.getLogger(SummarizeTest.class);

    @Inject
    private Environment env;
    @Inject
    private ScrapeTemplateRepositoryImpl scrapeTemplateRepo;
    @Inject
    private Summarizer summarizer;
    private ScrapeTemplate schoolSelect;
    private ScrapeTemplate registeredSelect;
    private ScrapeTemplate studentGet;
    private ScrapeTemplate filteredSelect;

    @Configuration
    @ComponentScan(basePackageClasses = Fetcher.class)
    public static class Config {
    }

    @Before
    public void setUp() {
        schoolSelect = scrapeTemplateRepo.add(new File(
                "sample/ppdb/school_select.ScrapeTemplate.jsonld"));
        registeredSelect = scrapeTemplateRepo.add(new File(
                "sample/ppdb/registered_select.ScrapeTemplate.jsonld"));
        filteredSelect = scrapeTemplateRepo.add(new File(
                "sample/ppdb/filtered_select.ScrapeTemplate.jsonld"));
        studentGet = scrapeTemplateRepo.add(new File(
                "sample/ppdb/student_get.ScrapeTemplate.jsonld"));
    }

    @Test
    public void summarizeFixed() throws IOException, PropertyVetoException {
        final ComboPooledDataSource dataSource = new ComboPooledDataSource();
        try {
            dataSource.setDriverClass(Driver.class.getName());
            dataSource.setJdbcUrl("jdbc:postgresql://localhost/scrape_scrape_dev");
            dataSource.setUser("postgres");
            dataSource.setPassword("bippo");

            final DataSourceTransactionManager txMgr = new DataSourceTransactionManager(dataSource);
            txMgr.afterPropertiesSet();

            // Please delete this data point, we already have 2015-07-02T04:00+07:00 generated manually for reference
            summarizer.summarize("ppdbbandung2015", "optionapplicantsnapshot", dataSource,
                    txMgr, new DateTime("2015-07-02T05:00+07:00"));
        } finally {
            dataSource.close();
        }
    }

}
