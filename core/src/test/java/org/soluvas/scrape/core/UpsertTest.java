package org.soluvas.scrape.core;

import com.google.common.collect.ImmutableMap;
import com.mchange.v2.c3p0.ComboPooledDataSource;
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
@ActiveProfiles({"sql", "scraper"})
public class UpsertTest {

    private static final Logger log = LoggerFactory.getLogger(UpsertTest.class);

    @Inject
    private Environment env;
    @Inject
    private ScrapeTemplateRepositoryImpl scrapeTemplateRepo;
    @Inject
    private Fetcher fetcher;
    @Inject
    private Scraper scraper;
    @Inject
    private TableDmlGenerator tableDmlGenerator;
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
    public void upsertSchool() throws IOException, PropertyVetoException {
        final FetchData result = fetcher.fetch(schoolSelect,
                ImmutableMap.of("level", "senior"));
        final ScrapeData scrapeData = scraper.scrape(schoolSelect, result);

        final ComboPooledDataSource dataSource = new ComboPooledDataSource();
        try {
            dataSource.setDriverClass(Driver.class.getName());
            dataSource.setJdbcUrl("jdbc:postgresql://localhost/scrape_scrape_dev");
            dataSource.setUser("postgres");
            dataSource.setPassword("bippo");

            final DataSourceTransactionManager txMgr = new DataSourceTransactionManager(dataSource);
            txMgr.afterPropertiesSet();

            tableDmlGenerator.upsert("ppdbbandung2015", schoolSelect,
                    scrapeData, dataSource, txMgr);
        } finally {
            dataSource.close();
        }
    }

    @Test
    public void upsertRegistered() throws IOException, PropertyVetoException {
        final FetchData result = fetcher.fetch(registeredSelect,
                ImmutableMap.of("choice_id", 615));
        final ScrapeData scrapeData = scraper.scrape(registeredSelect, result);

        final ComboPooledDataSource dataSource = new ComboPooledDataSource();
        try {
            dataSource.setDriverClass(Driver.class.getName());
            dataSource.setJdbcUrl("jdbc:postgresql://localhost/scrape_scrape_dev");
            dataSource.setUser("postgres");
            dataSource.setPassword("bippo");

            final DataSourceTransactionManager txMgr = new DataSourceTransactionManager(dataSource);
            txMgr.afterPropertiesSet();

            tableDmlGenerator.upsert("ppdbbandung2015", registeredSelect,
                    scrapeData, dataSource, txMgr);
        } finally {
            dataSource.close();
        }
    }

    @Test
    public void upsertFiltered() throws IOException, PropertyVetoException {
        final FetchData result = fetcher.fetch(filteredSelect,
                ImmutableMap.of("choice_id", 685));
        final ScrapeData scrapeData = scraper.scrape(filteredSelect, result);

        final ComboPooledDataSource dataSource = new ComboPooledDataSource();
        try {
            dataSource.setDriverClass(Driver.class.getName());
            dataSource.setJdbcUrl("jdbc:postgresql://localhost/scrape_scrape_dev");
            dataSource.setUser("postgres");
            dataSource.setPassword("bippo");

            final DataSourceTransactionManager txMgr = new DataSourceTransactionManager(dataSource);
            txMgr.afterPropertiesSet();

            tableDmlGenerator.upsert("ppdbbandung2015", filteredSelect,
                    scrapeData, dataSource, txMgr);
        } finally {
            dataSource.close();
        }
    }

    @Test
    public void upsertStudent() throws IOException, PropertyVetoException {
        final FetchData result = fetcher.fetch(studentGet,
                ImmutableMap.of("registration_id", "3004-1009afirmasi"));
        final ScrapeData scrapeData = scraper.scrape(studentGet, result);

        final ComboPooledDataSource dataSource = new ComboPooledDataSource();
        try {
            dataSource.setDriverClass(Driver.class.getName());
            dataSource.setJdbcUrl("jdbc:postgresql://localhost/scrape_scrape_dev");
            dataSource.setUser("postgres");
            dataSource.setPassword("bippo");

            final DataSourceTransactionManager txMgr = new DataSourceTransactionManager(dataSource);
            txMgr.afterPropertiesSet();

            tableDmlGenerator.upsert("ppdbbandung2015", studentGet,
                    scrapeData, dataSource, txMgr);
        } finally {
            dataSource.close();
        }
    }

}
