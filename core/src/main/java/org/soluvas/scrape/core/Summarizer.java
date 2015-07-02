package org.soluvas.scrape.core;

import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.Timestamp;

/**
 * Summarize data and snapshot historical data as time series data mart/fact table.
 * Created by ceefour on 7/2/15.
 */
@Service
@Profile("summarizer")
public class Summarizer {

    private static final Logger log = LoggerFactory.getLogger(Summarizer.class);
    
    /**
     *
     * @param schemaName
     * @param factTableName "Fact table" in data warehouse-speak.
     */
    public void summarize(String schemaName, String factTableName,
                          DataSource dataSource, PlatformTransactionManager txMgr,
                          DateTime snapshotTime) {
        final TransactionTemplate txTemplate = new TransactionTemplate(txMgr);
        final NamedParameterJdbcTemplate jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        txTemplate.execute(tx -> {
            step1insertPlaceholders(schemaName, factTableName, "school",
                    jdbcTemplate, tx, snapshotTime);
            step2updateFromOption(schemaName, factTableName,
                    jdbcTemplate, tx, snapshotTime);
            step3updateFromApplicant(schemaName, factTableName, "applicant",
                    jdbcTemplate, tx, snapshotTime);
            step4updateFromFilteredApplicant(schemaName, factTableName, "filteredapplicant",
                    jdbcTemplate, tx, snapshotTime);
            return null;
        });
        /*
        Step 0: CREATE TABLE

CREATE TABLE ppdbbandung2015.optionapplicantsnapshot (
    snapshottime timestamp with time zone,
    option_id int not null,
    registeredcount int,
    registeredforeignercount int,
    registeredforeignerdetailcount int,
    registeredinsidercount int,
    registeredinsentifcount int,
    registeredminscoretotal1 double precision,
    registeredq1scoretotal1 double precision,
    registeredq2scoretotal1 double precision,
    registeredq3scoretotal1 double precision,
    registeredmeanscoretotal1 double precision,
    registeredregistrationids varchar(255)[],
    registeredscoretotal1s double precision[],
    filteredcount int,
    filteredforeignercount int,
    filteredforeignerdetailcount int,
    filteredinsidercount int,
    filteredinsentifcount int,
    filteredminscoretotal1 double precision,
    filteredq1scoretotal1 double precision,
    filteredq2scoretotal1 double precision,
    filteredq3scoretotal1 double precision,
    filteredmeanscoretotal1 double precision,
    filteredregistrationids varchar(255)[],
    filteredscoretotal1s double precision[],
    PRIMARY KEY (snapshottime, option_id)
);
CREATE INDEX ik_optionapplicantsnapshot_snapshottime ON ppdbbandung2015.optionapplicantsnapshot (snapshottime);
CREATE INDEX ik_optionapplicantsnapshot_option_id ON ppdbbandung2015.optionapplicantsnapshot (option_id);

        Step 1: You INSERT necessary placeholder records
        WARNING: Nobody chooses SMA17 as first_choice, so we have to grab from filteredapplicant also.
        but the best is actually to grab from school.option

Best:

SELECT unnest(option)->'id' FROM ppdbbandung2015.school
-- INSERT INTO ppdbbandung2015.optionapplicantsnapshot (snapshottime, option_id)
SELECT '2015-07-02T05:00+07:00'::timestamp with time zone snapshottime,
  (unnest(option)->>'id')::integer option_id FROM ppdbbandung2015.school;

Obsolete:

-- INSERT INTO ppdbbandung2015.optionapplicantsnapshot (snapshottime, option_id)
SELECT '2015-07-02T04:00+07:00'::timestamp with time zone snapshottime, option_id
FROM ppdbbandung2015.applicant
GROUP BY option_id
ORDER BY option_id;

        Step 2: TODO: Grab option+school info

        Step 3: You grab data for the registered applicant. To be used for UPDATE FROM later.

WITH ranks AS (
	SELECT id rank_id, option_id rank_option_id, score_total1 rank_total1,
	  percent_rank() OVER (PARTITION BY option_id ORDER BY score_total1) AS percentrank,
	  ntile(4) OVER (PARTITION BY option_id ORDER BY score_total1) AS qtile
	FROM ppdbbandung2015.applicant
)
SELECT option_id, '2015-07-02T04:00+07:00'::timestamp with time zone snapshottime,
  COUNT(*) registeredcount,
  SUM(CASE WHEN is_foreigner=true THEN 1 ELSE 0 END) registeredforeignercount,
  SUM(CASE WHEN is_foreigner=false AND is_foreigner_detail=true THEN 1 ELSE 0 END) registeredforeignerdetailcount,
  SUM(CASE WHEN is_foreigner=false AND is_foreigner_detail=false THEN 1 ELSE 0 END) registeredinsidercount,
  SUM(CASE WHEN is_insentif=true THEN 1 ELSE 0 END) registeredinsentifcount,
  MIN(score_total1) registeredminscoretotal1,
  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND ranks.qtile=1) registeredq1scoretotal1,
  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND ranks.qtile=2) registeredq2scoretotal1,
  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND ranks.qtile=3) registeredq3scoretotal1,
  MAX(score_total1) registeredmaxscoretotal1,
  AVG(score_total1) registeredmeanscoretotal1,
  array_agg(id) registeredregistrationids,
  array_agg(score_total1) registeredscoretotal1s
FROM ppdbbandung2015.applicant
  JOIN ranks ON rank_id = id
GROUP BY option_id
ORDER BY option_id;

Update From template:

UPDATE ppdbbandung2015.optionapplicantsnapshot up
SET
    registeredcount=tmp.registeredcount,
    registeredforeignercount=tmp.registeredforeignercount,
    registeredforeignerdetailcount=tmp.registeredforeignerdetailcount,
    registeredinsidercount=tmp.registeredinsidercount,
    registeredinsentifcount=tmp.registeredinsentifcount,
    registeredminscoretotal1=tmp.registeredminscoretotal1,
    registeredq1scoretotal1=tmp.registeredq1scoretotal1,
    registeredq2scoretotal1=tmp.registeredq2scoretotal1,
    registeredq3scoretotal1=tmp.registeredq3scoretotal1,
    registeredmeanscoretotal1=tmp.registeredmeanscoretotal1,
    registeredregistrationids=tmp.registeredregistrationids,
    registeredscoretotal1s=tmp.registeredscoretotal1s
FROM (
...
) tmp
WHERE up.option_id=tmp.option_id AND up.snapshottime=tmp.snapshottime;

        Step 4: You grab data for the filteredapplicant. To be used for UPDATE USING later.

WITH ranks AS (
	SELECT id rank_id, option_id rank_option_id, score_total1 rank_total1, percent_rank() OVER (PARTITION BY option_id ORDER BY score_total1) AS percentrank,
	  ntile(4) OVER (PARTITION BY option_id ORDER BY score_total1) AS qtile
	FROM ppdbbandung2015.filteredapplicant
)
SELECT option_id, '2015-07-02T04:00+07:00'::timestamp with time zone snapshottime,
  COUNT(*) filteredcount,
  SUM(CASE WHEN is_foreigner=true THEN 1 ELSE 0 END) filteredforeignercount,
  SUM(CASE WHEN is_foreigner=false AND is_foreigner_detail=true THEN 1 ELSE 0 END) filteredforeignerdetailcount,
  SUM(CASE WHEN is_foreigner=false AND is_foreigner_detail=false THEN 1 ELSE 0 END) filteredinsidercount,
  SUM(CASE WHEN is_insentif=true THEN 1 ELSE 0 END) filteredinsentifcount,
  MIN(score_total1) filteredminscoretotal1,
  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND ranks.qtile=1) filteredq1scoretotal1,
  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND ranks.qtile=2) filteredq2scoretotal1,
  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND ranks.qtile=3) filteredq3scoretotal1,
  MAX(score_total1) filteredmaxscoretotal1,
  AVG(score_total1) filteredmeanscoretotal1,
  array_agg(id) filteredregistrationids,
  array_agg(score_total1) filteredscoretotal1s
FROM ppdbbandung2015.filteredapplicant
  JOIN ranks ON rank_id = id
GROUP BY option_id
ORDER BY option_id;

Update From template:

UPDATE ppdbbandung2015.optionapplicantsnapshot up
SET
    filteredcount=tmp.filteredcount,
    filteredforeignercount=tmp.filteredforeignercount,
    filteredforeignerdetailcount=tmp.filteredforeignerdetailcount,
    filteredinsidercount=tmp.filteredinsidercount,
    filteredinsentifcount=tmp.filteredinsentifcount,
    filteredminscoretotal1=tmp.filteredminscoretotal1,
    filteredq1scoretotal1=tmp.filteredq1scoretotal1,
    filteredq2scoretotal1=tmp.filteredq2scoretotal1,
    filteredq3scoretotal1=tmp.filteredq3scoretotal1,
    filteredmeanscoretotal1=tmp.filteredmeanscoretotal1,
    filteredregistrationids=tmp.filteredregistrationids,
    filteredscoretotal1s=tmp.filteredscoretotal1s
FROM (
...
) tmp
WHERE up.option_id=tmp.option_id AND up.snapshottime=tmp.snapshottime;

         */
    }

    /**
     * Step 1: You INSERT necessary placeholder records
     * MUST be executed inside Transaction
     */
    private void step1insertPlaceholders(String schemaName, String summaryTable,
                                         String schoolTable,
                                         NamedParameterJdbcTemplate jdbcTemplate,
                                         TransactionStatus tx,
                                         DateTime snapshotTime) {
        final String sql = "INSERT INTO " + schemaName + "." + summaryTable + " (snapshottime, option_id)\n" +
                "SELECT :snapshotTime::timestamp with time zone snapshottime," +
                "  (unnest(option)->>'id')::integer option_id\n" +
                "FROM " + schemaName + "." + schoolTable;
        log.info("Inserting placeholder for {}", snapshotTime);
        jdbcTemplate.update(sql, ImmutableMap.of("snapshotTime", snapshotTime.toString()));
    }
    
    /**
     * Step 2: TODO: Grab option+school info
     * MUST be executed inside Transaction
     */
    private void step2updateFromOption(String schemaName, String summaryTable,
                                       NamedParameterJdbcTemplate jdbcTemplate,
                                       TransactionStatus tx,
                                       DateTime snapshotTime) {
    }
    
    /**
     * Step 3: You grab data for the registered applicant. To be used for UPDATE FROM later.
     * MUST be executed inside Transaction
     */
    private void step3updateFromApplicant(String schemaName, String summaryTable,
                                         String applicantTable,
                                         NamedParameterJdbcTemplate jdbcTemplate,
                                         TransactionStatus tx,
                                         DateTime snapshotTime) {
        final String subSql = "WITH ranks AS (\n" +
                "\tSELECT id rank_id, option_id rank_option_id, score_total1 rank_total1, percent_rank() OVER (PARTITION BY option_id ORDER BY score_total1) AS percentrank,\n" +
                "\t  ntile(4) OVER (PARTITION BY option_id ORDER BY score_total1) AS qtile\n" +
                "\tFROM " + schemaName + "." + applicantTable +"\n" +
                ")\n" +
                "SELECT :snapshotTime::timestamp with time zone snapshottime, option_id,\n" +
                "  COUNT(*) registeredcount,\n" +
                "  SUM(CASE WHEN is_foreigner=true THEN 1 ELSE 0 END) registeredforeignercount,\n" +
                "  SUM(CASE WHEN is_foreigner=false AND is_foreigner_detail=true THEN 1 ELSE 0 END) registeredforeignerdetailcount,\n" +
                "  SUM(CASE WHEN is_foreigner=false AND is_foreigner_detail=false THEN 1 ELSE 0 END) registeredinsidercount,\n" +
                "  SUM(CASE WHEN is_insentif=true THEN 1 ELSE 0 END) registeredinsentifcount,\n" +
                "  MIN(score_total1) registeredminscoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND ranks.qtile=1) registeredq1scoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND ranks.qtile=2) registeredq2scoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND ranks.qtile=3) registeredq3scoretotal1,\n" +
                "  MAX(score_total1) registeredmaxscoretotal1,\n" +
                "  AVG(score_total1) registeredmeanscoretotal1,\n" +
                "  array_agg(id) registeredregistrationids,\n" +
                "  array_agg(score_total1) registeredscoretotal1s\n" +
                "FROM "+ schemaName + "."+ applicantTable + "\n" +
                "  JOIN ranks ON rank_id = id\n" +
                "GROUP BY option_id\n" +
                "ORDER BY option_id";
        
        final String sql = "UPDATE " + schemaName + "." + summaryTable + " up\n" +
                "SET\n" +
                "    registeredcount=tmp.registeredcount,\n" +
                "    registeredforeignercount=tmp.registeredforeignercount,\n" +
                "    registeredforeignerdetailcount=tmp.registeredforeignerdetailcount,\n" +
                "    registeredinsidercount=tmp.registeredinsidercount,\n" +
                "    registeredinsentifcount=tmp.registeredinsentifcount,\n" +
                "    registeredminscoretotal1=tmp.registeredminscoretotal1,\n" +
                "    registeredq1scoretotal1=tmp.registeredq1scoretotal1,\n" +
                "    registeredq2scoretotal1=tmp.registeredq2scoretotal1,\n" +
                "    registeredq3scoretotal1=tmp.registeredq3scoretotal1,\n" +
                "    registeredmeanscoretotal1=tmp.registeredmeanscoretotal1,\n" +
                "    registeredregistrationids=tmp.registeredregistrationids,\n" +
                "    registeredscoretotal1s=tmp.registeredscoretotal1s\n" +
                "FROM (\n" +
                subSql + "\n" +
                ") tmp\n" +
                "WHERE up.snapshottime=tmp.snapshottime AND up.option_id=tmp.option_id;\n";
        log.info("Updating registered applicant in table {}.{} for {}",
                schemaName, applicantTable, snapshotTime);
        jdbcTemplate.update(sql, ImmutableMap.of("snapshotTime", snapshotTime.toString()));
    }
    
    /**
     * Step 4: You grab data for the filtered applicant. To be used for UPDATE FROM later.
     * MUST be executed inside Transaction
     */
    private void step4updateFromFilteredApplicant(String schemaName, String summaryTable,
                                         String filteredApplicantTable,
                                         NamedParameterJdbcTemplate jdbcTemplate,
                                         TransactionStatus tx,
                                         DateTime snapshotTime) {
        final String subSql = "WITH ranks AS (\n" +
                "\tSELECT id rank_id, option_id rank_option_id, score_total1 rank_total1, percent_rank() OVER (PARTITION BY option_id ORDER BY score_total1) AS percentrank,\n" +
                "\t  ntile(4) OVER (PARTITION BY option_id ORDER BY score_total1) AS qtile\n" +
                "\tFROM " + schemaName + "." + filteredApplicantTable +"\n" +
                ")\n" +
                "SELECT :snapshotTime::timestamp with time zone snapshottime, option_id,\n" +
                "  COUNT(*) filteredcount,\n" +
                "  SUM(CASE WHEN is_foreigner=true THEN 1 ELSE 0 END) filteredforeignercount,\n" +
                "  SUM(CASE WHEN is_foreigner=false AND is_foreigner_detail=true THEN 1 ELSE 0 END) filteredforeignerdetailcount,\n" +
                "  SUM(CASE WHEN is_foreigner=false AND is_foreigner_detail=false THEN 1 ELSE 0 END) filteredinsidercount,\n" +
                "  SUM(CASE WHEN is_insentif=true THEN 1 ELSE 0 END) filteredinsentifcount,\n" +
                "  MIN(score_total1) filteredminscoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND ranks.qtile=1) filteredq1scoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND ranks.qtile=2) filteredq2scoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND ranks.qtile=3) filteredq3scoretotal1,\n" +
                "  MAX(score_total1) filteredmaxscoretotal1,\n" +
                "  AVG(score_total1) filteredmeanscoretotal1,\n" +
                "  array_agg(id) filteredregistrationids,\n" +
                "  array_agg(score_total1) filteredscoretotal1s\n" +
                "FROM "+ schemaName + "."+ filteredApplicantTable + "\n" +
                "  JOIN ranks ON rank_id = id\n" +
                "GROUP BY option_id\n" +
                "ORDER BY option_id";
        
        final String sql = "UPDATE " + schemaName + "." + summaryTable + " up\n" +
                "SET\n" +
                "    filteredcount=tmp.filteredcount,\n" +
                "    filteredforeignercount=tmp.filteredforeignercount,\n" +
                "    filteredforeignerdetailcount=tmp.filteredforeignerdetailcount,\n" +
                "    filteredinsidercount=tmp.filteredinsidercount,\n" +
                "    filteredinsentifcount=tmp.filteredinsentifcount,\n" +
                "    filteredminscoretotal1=tmp.filteredminscoretotal1,\n" +
                "    filteredq1scoretotal1=tmp.filteredq1scoretotal1,\n" +
                "    filteredq2scoretotal1=tmp.filteredq2scoretotal1,\n" +
                "    filteredq3scoretotal1=tmp.filteredq3scoretotal1,\n" +
                "    filteredmeanscoretotal1=tmp.filteredmeanscoretotal1,\n" +
                "    filteredregistrationids=tmp.filteredregistrationids,\n" +
                "    filteredscoretotal1s=tmp.filteredscoretotal1s\n" +
                "FROM (\n" +
                subSql + "\n" +
                ") tmp\n" +
                "WHERE up.snapshottime=tmp.snapshottime AND up.option_id=tmp.option_id;\n";
        log.info("Updating filtered applicant in table {}.{} for {}",
                schemaName, filteredApplicantTable, snapshotTime);
        jdbcTemplate.update(sql, ImmutableMap.of("snapshotTime", snapshotTime.toString()));
    }
    
}
