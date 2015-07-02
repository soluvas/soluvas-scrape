package org.soluvas.scrape.core;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.soluvas.scrape.core.sql.TableDdlGenerator;
import org.springframework.boot.test.ConfigFileApplicationContextInitializer;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;

/**
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(initializers = ConfigFileApplicationContextInitializer.class)
@ActiveProfiles("sql")
public class TableDdlGeneratorTest {

    private static final Logger log = LoggerFactory.getLogger(TableDdlGeneratorTest.class);

    @Inject
    private Environment env;
    @Inject
    private ScrapeTemplateRepositoryImpl scrapeTemplateRepo;
    @Inject
    private TableDdlGenerator tableDdlGenerator;
    private ScrapeTemplate schoolSelect;
    private ScrapeTemplate registeredSelect;
    private ScrapeTemplate studentGet;
    private ScrapeTemplate filteredSelect;

    @Configuration
    @ComponentScan(basePackageClasses = Fetcher.class)
    public static class Config {}

    @Before
    public void setUp() {
        schoolSelect = scrapeTemplateRepo.add(new File(
                "sample/ppdb/school_select.ScrapeTemplate.jsonld"
        ));
        registeredSelect = scrapeTemplateRepo.add(new File(
                "sample/ppdb/registered_select.ScrapeTemplate.jsonld"));
        filteredSelect = scrapeTemplateRepo.add(new File(
                "sample/ppdb/filtered_select.ScrapeTemplate.jsonld"));
        studentGet = scrapeTemplateRepo.add(new File(
                "sample/ppdb/student_get.ScrapeTemplate.jsonld"));
    }

    @Test
    public void generateCreateTable_schoolSelect() throws IOException {
        final String sql = tableDdlGenerator.generateCreateTable("ppdbbandung2015", schoolSelect);
        log.info("SQL: {}", sql);
    }

    @Test
    public void generateCreateTable_registeredSelect() throws IOException {
        final String sql = tableDdlGenerator.generateCreateTable("ppdbbandung2015", registeredSelect);
        log.info("SQL: {}", sql);
    }

    @Test
    public void generateCreateTable_filteredSelect() throws IOException {
        final String sql = tableDdlGenerator.generateCreateTable("ppdbbandung2015", filteredSelect);
        log.info("SQL: {}", sql);
    }

    @Test
    public void generateCreateTable_studentGet() throws IOException {
        final String sql = tableDdlGenerator.generateCreateTable("ppdbbandung2015", studentGet);
        log.info("SQL: {}", sql);
    }

}
