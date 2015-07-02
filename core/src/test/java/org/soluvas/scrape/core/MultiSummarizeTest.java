package org.soluvas.scrape.core;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.postgresql.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.soluvas.json.JsonUtils;
import org.soluvas.scrape.core.sql.TableDmlGenerator;
import org.springframework.boot.test.ConfigFileApplicationContextInitializer;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.support.TransactionTemplate;

import javax.inject.Inject;
import java.beans.PropertyVetoException;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(initializers = ConfigFileApplicationContextInitializer.class)
@ActiveProfiles({"sql", "scraper", "summarizer"})
public class MultiSummarizeTest {

    private static final Logger log = LoggerFactory.getLogger(MultiSummarizeTest.class);

    @Inject
    private Environment env;
    @Inject
    private ScrapeTemplateRepositoryImpl scrapeTemplateRepo;
    @Inject
    private Fetcher fetcher;
    @Inject
    private Scraper scraper;
    @Inject
    private Summarizer summarizer;
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
    public void multiScrapeThenSummarizeFromSchoolOptions() throws IOException, PropertyVetoException {
        final ComboPooledDataSource dataSource = new ComboPooledDataSource();
        try {
            dataSource.setDriverClass(Driver.class.getName());
            dataSource.setJdbcUrl("jdbc:postgresql://localhost/scrape_scrape_dev");
            dataSource.setUser("postgres");
            dataSource.setPassword("bippo");

            final DataSourceTransactionManager txMgr = new DataSourceTransactionManager(dataSource);
            txMgr.afterPropertiesSet();

            final NamedParameterJdbcTemplate jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
            final List<Set<Integer>> nestedOptionIds = new TransactionTemplate(txMgr).execute(tx -> {
                final List<Set<Integer>> optionIds = jdbcTemplate.query("SELECT option::text[] FROM ppdbbandung2015.school", (rs, rowNum) -> {
                    final String[] arr = (String[]) rs.getArray("option").getArray();
                    return Arrays.stream(arr).map((String optionJsonStr) -> {
                        try {
                            final ObjectNode optionObj = JsonUtils.mapper.readValue(optionJsonStr, ObjectNode.class);
                            final int optionId = optionObj.get("id").asInt();
                            return optionId;
                        } catch (IOException e) {
                            throw new ScrapeException(e);
                        }
                    }).collect(Collectors.toSet());
                });
                log.info("Option IDs: {}", optionIds);
                return optionIds;
            });
            log.info("Nested option IDs from {} schools: {}", nestedOptionIds.size(), nestedOptionIds);

            final Set<Integer> aggregateOptionIds = nestedOptionIds.stream().flatMap(Set::stream).collect(Collectors.toSet());
            log.info("{} aggregate option IDs: {}", aggregateOptionIds.size(), aggregateOptionIds);

            // REGISTERED
            for (Integer optionId : aggregateOptionIds) {
                final FetchData result = fetcher.fetch(registeredSelect,
                        ImmutableMap.of("choice_id", optionId));
                final ScrapeData scrapeData = scraper.scrape(registeredSelect, result);

                tableDmlGenerator.upsert("ppdbbandung2015", registeredSelect,
                        scrapeData, dataSource, txMgr);
            }
            // FILTERED
            // FIXME: Workaround because we don't know how to remove old stuff yet
            jdbcTemplate.update("DELETE FROM ppdbbandung2015.filteredapplicant", ImmutableMap.of());
            for (Integer optionId : aggregateOptionIds) {
                final FetchData result = fetcher.fetch(filteredSelect,
                        ImmutableMap.of("choice_id", optionId));
                final ScrapeData scrapeData = scraper.scrape(filteredSelect, result);
                tableDmlGenerator.upsert("ppdbbandung2015", filteredSelect,
                        scrapeData, dataSource, txMgr);
            }

            summarizer.summarize("ppdbbandung2015", "optionapplicantsnapshot", dataSource,
                    txMgr, new DateTime());
        } finally {
            dataSource.close();
        }
    }

}
