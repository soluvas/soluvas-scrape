package org.soluvas.scrape.core.cli;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.soluvas.json.JsonUtils;
import org.soluvas.scrape.core.*;
import org.soluvas.scrape.core.sql.TableDmlGenerator;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.beans.PropertyVetoException;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by ceefour on 7/6/15.
 */
public class MultiScrapeThenSummarize implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(MultiScrapeThenSummarize.class);

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

    public static void main(String... args) {
        final SpringApplicationBuilder builder = new SpringApplicationBuilder()
                .sources(MultiScrapeThenSummarize.Config.class);
        builder.application().setWebEnvironment(false);
        builder.profiles("sql", "scraper", "summarizer");
        builder.run(args);
    }

    @ComponentScan(basePackageClasses = Fetcher.class)
    @Configuration
    public static class Config {
        @Bean
        public MultiScrapeThenSummarize multiScrapeThenSummarize() {
            return new MultiScrapeThenSummarize();
        }
    }

    @PostConstruct
    public void init() {
        schoolSelect = scrapeTemplateRepo.add(new File(
                "sample/ppdb/school_select.ScrapeTemplate.jsonld"));
        registeredSelect = scrapeTemplateRepo.add(new File(
                "sample/ppdb/registered_select.ScrapeTemplate.jsonld"));
        filteredSelect = scrapeTemplateRepo.add(new File(
                "sample/ppdb/filtered_select.ScrapeTemplate.jsonld"));
        studentGet = scrapeTemplateRepo.add(new File(
                "sample/ppdb/student_get.ScrapeTemplate.jsonld"));
    }

    @Override
    public void run(String... args) throws Exception {
        periodicMultiScrapeThenSummarizeFromSchoolOptions();
    }

    public void multiScrapeThenSummarizeFromSchoolOptions() throws IOException, PropertyVetoException {
        final ComboPooledDataSource dataSource = new ComboPooledDataSource();
        try {
            dataSource.setDriverClass(env.getRequiredProperty("spring.datasource.driverClassName"));
            dataSource.setJdbcUrl(env.getRequiredProperty("spring.datasource.url"));
            dataSource.setUser(env.getRequiredProperty("spring.datasource.username"));
            dataSource.setPassword(env.getRequiredProperty("spring.datasource.password"));

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

            final Set<String> alltimeRegistrationIds = new TransactionTemplate(txMgr).execute(tx -> {
                final Set<String> tmpRegistrationIds = ImmutableSet.copyOf(jdbcTemplate.queryForList(
                        "SELECT id FROM ppdbbandung2015.applicant ORDER BY id",
                        ImmutableMap.of(), String.class));
                log.info("{} Previous Applicant Registration IDs", tmpRegistrationIds.size());
                return tmpRegistrationIds;
            });

            // REGISTERED
            final LinkedHashSet<String> newRegistrationIds = new LinkedHashSet<>();
            for (Integer optionId : aggregateOptionIds) {
                final FetchData result = fetcher.fetch(registeredSelect,
                        ImmutableMap.of("choice_id", optionId));
                final ScrapeData scrapeData = scraper.scrape(registeredSelect, result);

                tableDmlGenerator.upsert("ppdbbandung2015", registeredSelect,
                        scrapeData, dataSource, txMgr);

                final CollectionData applicantColl = scrapeData.getCollections().stream()
                        .filter(it -> "applicant".equals(it.getDefinition().getId())).findAny().get();
                applicantColl.getEntities()
                        .forEach(it -> newRegistrationIds.add(it.getId()));
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

            final Sets.SetView<String> revokedRegistrationIds = Sets.difference(alltimeRegistrationIds, newRegistrationIds);
            log.info("All-time regIDs: {}. Current regIDs: {}. {} missing: {}",
                    alltimeRegistrationIds.size(), newRegistrationIds.size(),
                    revokedRegistrationIds.size(), revokedRegistrationIds);
            final String revokedSql = "SELECT * FROM ppdbbandung2015.student WHERE registration_id IN (" +
                    Joiner.on(", ").join(revokedRegistrationIds.stream().map(it -> "'" + it + "'").toArray()) + ")";
            log.info("{} missing regIDs: {}", revokedRegistrationIds.size(), revokedSql);
        } finally {
            dataSource.close();
        }
    }

    public void periodicMultiScrapeThenSummarizeFromSchoolOptions() throws IOException, PropertyVetoException, InterruptedException {
        while (true) {
            try {
                multiScrapeThenSummarizeFromSchoolOptions();
            } catch (Exception e) {
                log.error("Error but still go on", e);
            }
            log.info("Waiting 15 minutes...");
            Thread.sleep(15 * 60 * 1000);
        }
    }

}
