package org.soluvas.scrape.core;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.mchange.v2.c3p0.ComboPooledDataSource;
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
import java.io.Serializable;
import java.sql.Array;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(initializers = ConfigFileApplicationContextInitializer.class)
@ActiveProfiles({"sql", "scraper"})
public class MultiUpsertTest {

    private static final Logger log = LoggerFactory.getLogger(MultiUpsertTest.class);

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
    public void multiUpsertApplicantFromSchoolOptions() throws IOException, PropertyVetoException {
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

            final Set<Integer> aggregateOptionIds = nestedOptionIds.stream().flatMap(Set::stream).collect(Collectors.toSet());
            for (Integer optionId : aggregateOptionIds) {
                final FetchData result = fetcher.fetch(registeredSelect,
                        ImmutableMap.of("choice_id", optionId));
                final ScrapeData scrapeData = scraper.scrape(registeredSelect, result);

                tableDmlGenerator.upsert("ppdbbandung2015", registeredSelect,
                        scrapeData, dataSource, txMgr);
            }
        } finally {
            dataSource.close();
        }
    }

    @Test
    public void multiUpsertFilteredApplicantFromSchoolOptions() throws IOException, PropertyVetoException {
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

            final Set<Integer> aggregateOptionIds = nestedOptionIds.stream().flatMap(Set::stream).collect(Collectors.toSet());
            for (Integer optionId : aggregateOptionIds) {
                final FetchData result = fetcher.fetch(filteredSelect,
                        ImmutableMap.of("choice_id", optionId));
                final ScrapeData scrapeData = scraper.scrape(filteredSelect, result);
                tableDmlGenerator.upsert("ppdbbandung2015", filteredSelect,
                        scrapeData, dataSource, txMgr);
            }
        } finally {
            dataSource.close();
        }
    }

    @Test
    public void multiUpsertStudentFromApplicant() throws IOException, PropertyVetoException {
        final ComboPooledDataSource dataSource = new ComboPooledDataSource();
        try {
            dataSource.setDriverClass(Driver.class.getName());
            dataSource.setJdbcUrl("jdbc:postgresql://localhost/scrape_scrape_dev");
            dataSource.setUser("postgres");
            dataSource.setPassword("bippo");

            final DataSourceTransactionManager txMgr = new DataSourceTransactionManager(dataSource);
            txMgr.afterPropertiesSet();

            final NamedParameterJdbcTemplate jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
            final Set<String> registrationIds = new TransactionTemplate(txMgr).execute(tx -> {
                final Set<String> tmpRegistrationIds = ImmutableSet.copyOf(jdbcTemplate.queryForList(
                        "SELECT id FROM ppdbbandung2015.applicant",
                        ImmutableMap.of(), String.class));
                log.info("{} Applicant Registration IDs: {}", tmpRegistrationIds.size(), tmpRegistrationIds);
                return tmpRegistrationIds;
            });
            final Set<String> existingStudentRegistrationIds = new TransactionTemplate(txMgr).execute(tx -> {
                final Set<String> tmpStudentRegistrationIds = ImmutableSet.copyOf(jdbcTemplate.queryForList(
                        "SELECT registration_id FROM ppdbbandung2015.student",
                        ImmutableMap.of(), String.class));
                log.info("{} Existing students: {}", tmpStudentRegistrationIds.size(), tmpStudentRegistrationIds);
                return tmpStudentRegistrationIds;
            });
            final Sets.SetView<String> missingRegistrationIds = Sets.difference(registrationIds, existingStudentRegistrationIds);
            log.info("{} missing out of {} registration IDs ({} existing students): {}",
                    missingRegistrationIds.size(), registrationIds.size(), existingStudentRegistrationIds.size(),
                    missingRegistrationIds);

            for (final String registrationId : missingRegistrationIds) {
                final FetchData result = fetcher.fetch(studentGet,
                        ImmutableMap.of("registration_id", registrationId));
                final ScrapeData scrapeData = scraper.scrape(studentGet, result);
                tableDmlGenerator.upsert("ppdbbandung2015", studentGet,
                        scrapeData, dataSource, txMgr);
            }
        } finally {
            dataSource.close();
        }
    }

}
