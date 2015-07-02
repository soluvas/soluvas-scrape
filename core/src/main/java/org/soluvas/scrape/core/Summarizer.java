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
    registeredstddevscoretotal1 double precision,
    registeredlowscoretotal1 double precision,
    registeredhighscoretotal1 double precision,

    registeredforeignerminscoretotal1 double precision,
    registeredforeignerq1scoretotal1 double precision,
    registeredforeignerq2scoretotal1 double precision,
    registeredforeignerq3scoretotal1 double precision,
    registeredforeignermeanscoretotal1 double precision,
    registeredforeignerstddevscoretotal1 double precision,
    registeredforeignerlowscoretotal1 double precision,
    registeredforeignerhighscoretotal1 double precision,

    registeredforeignerdetailminscoretotal1 double precision,
    registeredforeignerdetailq1scoretotal1 double precision,
    registeredforeignerdetailq2scoretotal1 double precision,
    registeredforeignerdetailq3scoretotal1 double precision,
    registeredforeignerdetailmeanscoretotal1 double precision,
    registeredforeignerdetailstddevscoretotal1 double precision,
    registeredforeignerdetaillowscoretotal1 double precision,
    registeredforeignerdetailhighscoretotal1 double precision,

    registeredinsiderminscoretotal1 double precision,
    registeredinsiderq1scoretotal1 double precision,
    registeredinsiderq2scoretotal1 double precision,
    registeredinsiderq3scoretotal1 double precision,
    registeredinsidermeanscoretotal1 double precision,
    registeredinsiderstddevscoretotal1 double precision,
    registeredinsiderlowscoretotal1 double precision,
    registeredinsiderhighscoretotal1 double precision,

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
    filteredstddevscoretotal1 double precision,
    filteredlowscoretotal1 double precision,
    filteredhighscoretotal1 double precision,

    filteredforeignerminscoretotal1 double precision,
    filteredforeignerq1scoretotal1 double precision,
    filteredforeignerq2scoretotal1 double precision,
    filteredforeignerq3scoretotal1 double precision,
    filteredforeignermeanscoretotal1 double precision,
    filteredforeignerstddevscoretotal1 double precision,
    filteredforeignerlowscoretotal1 double precision,
    filteredforeignerhighscoretotal1 double precision,

    filteredforeignerdetailminscoretotal1 double precision,
    filteredforeignerdetailq1scoretotal1 double precision,
    filteredforeignerdetailq2scoretotal1 double precision,
    filteredforeignerdetailq3scoretotal1 double precision,
    filteredforeignerdetailmeanscoretotal1 double precision,
    filteredforeignerdetailstddevscoretotal1 double precision,
    filteredforeignerdetaillowscoretotal1 double precision,
    filteredforeignerdetailhighscoretotal1 double precision,

    filteredinsiderminscoretotal1 double precision,
    filteredinsiderq1scoretotal1 double precision,
    filteredinsiderq2scoretotal1 double precision,
    filteredinsiderq3scoretotal1 double precision,
    filteredinsidermeanscoretotal1 double precision,
    filteredinsiderstddevscoretotal1 double precision,
    filteredinsiderlowscoretotal1 double precision,
    filteredinsiderhighscoretotal1 double precision,

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
	  is_foreigner rank_is_foreigner,
	  is_foreigner_detail rank_is_foreigner_detail,
	  is_insentif rank_is_insentif,
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
    registeredstddevscoretotal1=tmp.registeredstddevscoretotal1,
    registeredlowscoretotal1 = tmp.registeredmeanscoretotal1 - tmp.registeredstddevscoretotal1,
    registeredhighscoretotal1 = tmp.registeredmeanscoretotal1 + tmp.registeredstddevscoretotal1,

    registeredforeignerminscoretotal1=tmp.registeredforeignerminscoretotal1,
    registeredforeignerq1scoretotal1=tmp.registeredforeignerq1scoretotal1,
    registeredforeignerq2scoretotal1=tmp.registeredforeignerq2scoretotal1,
    registeredforeignerq3scoretotal1=tmp.registeredforeignerq3scoretotal1,
    registeredforeignermeanscoretotal1=tmp.registeredforeignermeanscoretotal1,
    registeredforeignerstddevscoretotal1=tmp.registeredforeignerstddevscoretotal1,
    registeredforeignerlowscoretotal1 = tmp.registeredforeignermeanscoretotal1 - tmp.registeredstddevscoretotal1,
    registeredforeignerhighscoretotal1 = tmp.registeredforeignermeanscoretotal1 + tmp.registeredstddevscoretotal1,

    registeredforeignerdetailminscoretotal1=tmp.registeredforeignerdetailminscoretotal1,
    registeredforeignerdetailq1scoretotal1=tmp.registeredforeignerdetailq1scoretotal1,
    registeredforeignerdetailq2scoretotal1=tmp.registeredforeignerdetailq2scoretotal1,
    registeredforeignerdetailq3scoretotal1=tmp.registeredforeignerdetailq3scoretotal1,
    registeredforeignerdetailmeanscoretotal1=tmp.registeredforeignerdetailmeanscoretotal1,
    registeredforeignerdetailstddevscoretotal1=tmp.registeredforeignerdetailstddevscoretotal1,
    registeredforeignerdetaillowscoretotal1 = tmp.registeredforeignerdetailmeanscoretotal1 - tmp.registeredstddevscoretotal1,
    registeredforeignerdetailhighscoretotal1 = tmp.registeredforeignerdetailmeanscoretotal1 + tmp.registeredstddevscoretotal1,

    registeredinsiderminscoretotal1=tmp.registeredinsiderminscoretotal1,
    registeredinsiderq1scoretotal1=tmp.registeredinsiderq1scoretotal1,
    registeredinsiderq2scoretotal1=tmp.registeredinsiderq2scoretotal1,
    registeredinsiderq3scoretotal1=tmp.registeredinsiderq3scoretotal1,
    registeredinsidermeanscoretotal1=tmp.registeredinsidermeanscoretotal1,
    registeredinsiderstddevscoretotal1=tmp.registeredinsiderstddevscoretotal1,
    registeredinsiderlowscoretotal1 = tmp.registeredinsidermeanscoretotal1 - tmp.registeredstddevscoretotal1,
    registeredinsiderhighscoretotal1 = tmp.registeredinsidermeanscoretotal1 + tmp.registeredstddevscoretotal1,

    registeredregistrationids=tmp.registeredregistrationids,
    registeredscoretotal1s=tmp.registeredscoretotal1s
FROM (
...
) tmp
WHERE up.option_id=tmp.option_id AND up.snapshottime=tmp.snapshottime;

        Step 4: You grab data for the filteredapplicant. To be used for UPDATE USING later.

WITH ranks AS (
	SELECT id rank_id, option_id rank_option_id, score_total1 rank_total1,
	  is_foreigner rank_is_foreigner,
	  is_foreigner_detail rank_is_foreigner_detail,
	  is_insentif rank_is_insentif,
	  percent_rank() OVER (PARTITION BY option_id ORDER BY score_total1) AS percentrank,
	  ntile(4) OVER (PARTITION BY option_id ORDER BY score_total1) AS qtile
	FROM ppdbbandung2015.filteredapplicant
)
SELECT '2015-07-02T04:00+07:00'::timestamp with time zone snapshottime, option_id,
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
  stddev_samp(score_total1) filteredstddevscoretotal1,
  
  (SELECT MIN(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=true) filteredforeignerminscoretotal1,
  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=true AND ranks.qtile=1) filteredforeignerq1scoretotal1,
  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=true AND ranks.qtile=2) filteredforeignerq2scoretotal1,
  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=true AND ranks.qtile=3) filteredforeignerq3scoretotal1,
  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=true) filteredforeignermaxscoretotal1,
  (SELECT AVG(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=true) filteredforeignermeanscoretotal1,
  (SELECT stddev_samp(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=true) filteredforeignerstddevscoretotal1,
  
  (SELECT MIN(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=false AND rank_is_foreigner_detail=true) filteredforeignerdetailminscoretotal1,
  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=false AND rank_is_foreigner_detail=true AND ranks.qtile=1) filteredforeignerdetailq1scoretotal1,
  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=false AND rank_is_foreigner_detail=true AND ranks.qtile=2) filteredforeignerdetailq2scoretotal1,
  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=false AND rank_is_foreigner_detail=true AND ranks.qtile=3) filteredforeignerdetailq3scoretotal1,
  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=false AND rank_is_foreigner_detail=true) filteredforeignerdetailmaxscoretotal1,
  (SELECT AVG(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=false AND rank_is_foreigner_detail=true) filteredforeignerdetailmeanscoretotal1,
  (SELECT stddev_samp(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=false AND rank_is_foreigner_detail=true) filteredforeignerdetailstddevscoretotal1,
  
  (SELECT MIN(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner_detail=false) filteredinsiderminscoretotal1,
  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner_detail=false AND ranks.qtile=1) filteredinsiderq1scoretotal1,
  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner_detail=false AND ranks.qtile=2) filteredinsiderq2scoretotal1,
  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner_detail=false AND ranks.qtile=3) filteredinsiderq3scoretotal1,
  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner_detail=false) filteredinsidermaxscoretotal1,
  (SELECT AVG(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner_detail=false) filteredinsidermeanscoretotal1,
  (SELECT stddev_samp(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner_detail=false) filteredinsiderstddevscoretotal1,
  
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
    filteredstddevscoretotal1=tmp.filteredstddevscoretotal1,
    filteredlowscoretotal1 = tmp.filteredmeanscoretotal1 - tmp.filteredstddevscoretotal1,
    filteredhighscoretotal1 = tmp.filteredmeanscoretotal1 + tmp.filteredstddevscoretotal1,

    filteredforeignerminscoretotal1=tmp.filteredforeignerminscoretotal1,
    filteredforeignerq1scoretotal1=tmp.filteredforeignerq1scoretotal1,
    filteredforeignerq2scoretotal1=tmp.filteredforeignerq2scoretotal1,
    filteredforeignerq3scoretotal1=tmp.filteredforeignerq3scoretotal1,
    filteredforeignermeanscoretotal1=tmp.filteredforeignermeanscoretotal1,
    filteredforeignerstddevscoretotal1=tmp.filteredforeignerstddevscoretotal1,
    filteredforeignerlowscoretotal1 = tmp.filteredforeignermeanscoretotal1 - tmp.filteredstddevscoretotal1,
    filteredforeignerhighscoretotal1 = tmp.filteredforeignermeanscoretotal1 + tmp.filteredstddevscoretotal1,

    filteredforeignerdetailminscoretotal1=tmp.filteredforeignerdetailminscoretotal1,
    filteredforeignerdetailq1scoretotal1=tmp.filteredforeignerdetailq1scoretotal1,
    filteredforeignerdetailq2scoretotal1=tmp.filteredforeignerdetailq2scoretotal1,
    filteredforeignerdetailq3scoretotal1=tmp.filteredforeignerdetailq3scoretotal1,
    filteredforeignerdetailmeanscoretotal1=tmp.filteredforeignerdetailmeanscoretotal1,
    filteredforeignerdetailstddevscoretotal1=tmp.filteredforeignerdetailstddevscoretotal1,
    filteredforeignerdetaillowscoretotal1 = tmp.filteredforeignerdetailmeanscoretotal1 - tmp.filteredstddevscoretotal1,
    filteredforeignerdetailhighscoretotal1 = tmp.filteredforeignerdetailmeanscoretotal1 + tmp.filteredstddevscoretotal1,

    filteredinsiderminscoretotal1=tmp.filteredinsiderminscoretotal1,
    filteredinsiderq1scoretotal1=tmp.filteredinsiderq1scoretotal1,
    filteredinsiderq2scoretotal1=tmp.filteredinsiderq2scoretotal1,
    filteredinsiderq3scoretotal1=tmp.filteredinsiderq3scoretotal1,
    filteredinsidermeanscoretotal1=tmp.filteredinsidermeanscoretotal1,
    filteredinsiderstddevscoretotal1=tmp.filteredinsiderstddevscoretotal1,
    filteredinsiderlowscoretotal1 = tmp.filteredinsidermeanscoretotal1 - tmp.filteredstddevscoretotal1,
    filteredinsiderhighscoretotal1 = tmp.filteredinsidermeanscoretotal1 + tmp.filteredstddevscoretotal1,

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
                "\tSELECT id rank_id, option_id rank_option_id, score_total1 rank_total1,\n" +
                "\t  is_foreigner rank_is_foreigner,\n" +
                "\t  is_foreigner_detail rank_is_foreigner_detail,\n" +
                "\t  is_insentif rank_is_insentif,\n" +
                "\t  percent_rank() OVER (PARTITION BY option_id ORDER BY score_total1) AS percentrank,\n" +
                "\t  ntile(4) OVER (PARTITION BY option_id ORDER BY score_total1) AS qtile\n" +
                "\tFROM " + schemaName + "." + applicantTable +"\n" +
                ")\n" +
                "SELECT :snapshotTime::timestamp with time zone snapshottime, option_id,\n" +
                "  COUNT(*) registeredcount,\n" +
                "  SUM(CASE WHEN is_foreigner=true THEN 1 ELSE 0 END) registeredforeignercount,\n" +
                "  SUM(CASE WHEN is_foreigner=false AND is_foreigner_detail=true THEN 1 ELSE 0 END) registeredforeignerdetailcount,\n" +
                "  SUM(CASE WHEN is_foreigner=false AND is_foreigner_detail=false THEN 1 ELSE 0 END) registeredinsidercount,\n" +
                "  SUM(CASE WHEN is_insentif=true THEN 1 ELSE 0 END) registeredinsentifcount,\n" +
                "  \n" +
                "  MIN(score_total1) registeredminscoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND ranks.qtile=1) registeredq1scoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND ranks.qtile=2) registeredq2scoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND ranks.qtile=3) registeredq3scoretotal1,\n" +
                "  MAX(score_total1) registeredmaxscoretotal1,\n" +
                "  AVG(score_total1) registeredmeanscoretotal1,\n" +
                "  stddev_samp(score_total1) registeredstddevscoretotal1,\n" +
                "  \n" +
                "  (SELECT MIN(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=true) registeredforeignerminscoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=true AND ranks.qtile=1) registeredforeignerq1scoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=true AND ranks.qtile=2) registeredforeignerq2scoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=true AND ranks.qtile=3) registeredforeignerq3scoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=true) registeredforeignermaxscoretotal1,\n" +
                "  (SELECT AVG(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=true) registeredforeignermeanscoretotal1,\n" +
                "  (SELECT stddev_samp(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=true) registeredforeignerstddevscoretotal1,\n" +
                "  \n" +
                "  (SELECT MIN(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=false AND rank_is_foreigner_detail=true) registeredforeignerdetailminscoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=false AND rank_is_foreigner_detail=true AND ranks.qtile=1) registeredforeignerdetailq1scoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=false AND rank_is_foreigner_detail=true AND ranks.qtile=2) registeredforeignerdetailq2scoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=false AND rank_is_foreigner_detail=true AND ranks.qtile=3) registeredforeignerdetailq3scoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=false AND rank_is_foreigner_detail=true) registeredforeignerdetailmaxscoretotal1,\n" +
                "  (SELECT AVG(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=false AND rank_is_foreigner_detail=true) registeredforeignerdetailmeanscoretotal1,\n" +
                "  (SELECT stddev_samp(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=false AND rank_is_foreigner_detail=true) registeredforeignerdetailstddevscoretotal1,\n" +
                "  \n" +
                "  (SELECT MIN(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner_detail=false) registeredinsiderminscoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner_detail=false AND ranks.qtile=1) registeredinsiderq1scoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner_detail=false AND ranks.qtile=2) registeredinsiderq2scoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner_detail=false AND ranks.qtile=3) registeredinsiderq3scoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner_detail=false) registeredinsidermaxscoretotal1,\n" +
                "  (SELECT AVG(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner_detail=false) registeredinsidermeanscoretotal1,\n" +
                "  (SELECT stddev_samp(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner_detail=false) registeredinsiderstddevscoretotal1,\n" +
                "  \n" +
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
                "    registeredstddevscoretotal1=tmp.registeredstddevscoretotal1,\n" +
                "    registeredlowscoretotal1 = tmp.registeredmeanscoretotal1 - tmp.registeredstddevscoretotal1,\n" +
                "    registeredhighscoretotal1 = tmp.registeredmeanscoretotal1 + tmp.registeredstddevscoretotal1,\n" +
                "\n" +
                "    registeredforeignerminscoretotal1=tmp.registeredforeignerminscoretotal1,\n" +
                "    registeredforeignerq1scoretotal1=tmp.registeredforeignerq1scoretotal1,\n" +
                "    registeredforeignerq2scoretotal1=tmp.registeredforeignerq2scoretotal1,\n" +
                "    registeredforeignerq3scoretotal1=tmp.registeredforeignerq3scoretotal1,\n" +
                "    registeredforeignermeanscoretotal1=tmp.registeredforeignermeanscoretotal1,\n" +
                "    registeredforeignerstddevscoretotal1=tmp.registeredforeignerstddevscoretotal1,\n" +
                "    registeredforeignerlowscoretotal1 = tmp.registeredforeignermeanscoretotal1 - tmp.registeredstddevscoretotal1,\n" +
                "    registeredforeignerhighscoretotal1 = tmp.registeredforeignermeanscoretotal1 + tmp.registeredstddevscoretotal1,\n" +
                "\n" +
                "    registeredforeignerdetailminscoretotal1=tmp.registeredforeignerdetailminscoretotal1,\n" +
                "    registeredforeignerdetailq1scoretotal1=tmp.registeredforeignerdetailq1scoretotal1,\n" +
                "    registeredforeignerdetailq2scoretotal1=tmp.registeredforeignerdetailq2scoretotal1,\n" +
                "    registeredforeignerdetailq3scoretotal1=tmp.registeredforeignerdetailq3scoretotal1,\n" +
                "    registeredforeignerdetailmeanscoretotal1=tmp.registeredforeignerdetailmeanscoretotal1,\n" +
                "    registeredforeignerdetailstddevscoretotal1=tmp.registeredforeignerdetailstddevscoretotal1,\n" +
                "    registeredforeignerdetaillowscoretotal1 = tmp.registeredforeignerdetailmeanscoretotal1 - tmp.registeredstddevscoretotal1,\n" +
                "    registeredforeignerdetailhighscoretotal1 = tmp.registeredforeignerdetailmeanscoretotal1 + tmp.registeredstddevscoretotal1,\n" +
                "\n" +
                "    registeredinsiderminscoretotal1=tmp.registeredinsiderminscoretotal1,\n" +
                "    registeredinsiderq1scoretotal1=tmp.registeredinsiderq1scoretotal1,\n" +
                "    registeredinsiderq2scoretotal1=tmp.registeredinsiderq2scoretotal1,\n" +
                "    registeredinsiderq3scoretotal1=tmp.registeredinsiderq3scoretotal1,\n" +
                "    registeredinsidermeanscoretotal1=tmp.registeredinsidermeanscoretotal1,\n" +
                "    registeredinsiderstddevscoretotal1=tmp.registeredinsiderstddevscoretotal1,\n" +
                "    registeredinsiderlowscoretotal1 = tmp.registeredinsidermeanscoretotal1 - tmp.registeredstddevscoretotal1,\n" +
                "    registeredinsiderhighscoretotal1 = tmp.registeredinsidermeanscoretotal1 + tmp.registeredstddevscoretotal1,\n" +
                "\n" +
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
                "\tSELECT id rank_id, option_id rank_option_id, score_total1 rank_total1,\n" +
                "\t  is_foreigner rank_is_foreigner,\n" +
                "\t  is_foreigner_detail rank_is_foreigner_detail,\n" +
                "\t  is_insentif rank_is_insentif,\n" +
                "\t  percent_rank() OVER (PARTITION BY option_id ORDER BY score_total1) AS percentrank,\n" +
                "\t  ntile(4) OVER (PARTITION BY option_id ORDER BY score_total1) AS qtile\n" +
                "\tFROM " + schemaName + "." + filteredApplicantTable +"\n" +
                ")\n" +
                "SELECT :snapshotTime::timestamp with time zone snapshottime, option_id,\n" +
                "  COUNT(*) filteredcount,\n" +
                "  SUM(CASE WHEN is_foreigner=true THEN 1 ELSE 0 END) filteredforeignercount,\n" +
                "  SUM(CASE WHEN is_foreigner=false AND is_foreigner_detail=true THEN 1 ELSE 0 END) filteredforeignerdetailcount,\n" +
                "  SUM(CASE WHEN is_foreigner=false AND is_foreigner_detail=false THEN 1 ELSE 0 END) filteredinsidercount,\n" +
                "  SUM(CASE WHEN is_insentif=true THEN 1 ELSE 0 END) filteredinsentifcount,\n" +
                "  \n" +
                "  MIN(score_total1) filteredminscoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND ranks.qtile=1) filteredq1scoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND ranks.qtile=2) filteredq2scoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND ranks.qtile=3) filteredq3scoretotal1,\n" +
                "  MAX(score_total1) filteredmaxscoretotal1,\n" +
                "  AVG(score_total1) filteredmeanscoretotal1,\n" +
                "  stddev_samp(score_total1) filteredstddevscoretotal1,\n" +
                "  \n" +
                "  (SELECT MIN(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=true) filteredforeignerminscoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=true AND ranks.qtile=1) filteredforeignerq1scoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=true AND ranks.qtile=2) filteredforeignerq2scoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=true AND ranks.qtile=3) filteredforeignerq3scoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=true) filteredforeignermaxscoretotal1,\n" +
                "  (SELECT AVG(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=true) filteredforeignermeanscoretotal1,\n" +
                "  (SELECT stddev_samp(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=true) filteredforeignerstddevscoretotal1,\n" +
                "  \n" +
                "  (SELECT MIN(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=false AND rank_is_foreigner_detail=true) filteredforeignerdetailminscoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=false AND rank_is_foreigner_detail=true AND ranks.qtile=1) filteredforeignerdetailq1scoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=false AND rank_is_foreigner_detail=true AND ranks.qtile=2) filteredforeignerdetailq2scoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=false AND rank_is_foreigner_detail=true AND ranks.qtile=3) filteredforeignerdetailq3scoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=false AND rank_is_foreigner_detail=true) filteredforeignerdetailmaxscoretotal1,\n" +
                "  (SELECT AVG(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=false AND rank_is_foreigner_detail=true) filteredforeignerdetailmeanscoretotal1,\n" +
                "  (SELECT stddev_samp(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner=false AND rank_is_foreigner_detail=true) filteredforeignerdetailstddevscoretotal1,\n" +
                "  \n" +
                "  (SELECT MIN(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner_detail=false) filteredinsiderminscoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner_detail=false AND ranks.qtile=1) filteredinsiderq1scoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner_detail=false AND ranks.qtile=2) filteredinsiderq2scoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner_detail=false AND ranks.qtile=3) filteredinsiderq3scoretotal1,\n" +
                "  (SELECT MAX(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner_detail=false) filteredinsidermaxscoretotal1,\n" +
                "  (SELECT AVG(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner_detail=false) filteredinsidermeanscoretotal1,\n" +
                "  (SELECT stddev_samp(rank_total1) FROM ranks WHERE rank_option_id=option_id AND rank_is_foreigner_detail=false) filteredinsiderstddevscoretotal1,\n" +
                "  \n" +
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
                "    filteredstddevscoretotal1=tmp.filteredstddevscoretotal1,\n" +
                "    filteredlowscoretotal1 = tmp.filteredmeanscoretotal1 - tmp.filteredstddevscoretotal1,\n" +
                "    filteredhighscoretotal1 = tmp.filteredmeanscoretotal1 + tmp.filteredstddevscoretotal1,\n" +
                "\n" +
                "    filteredforeignerminscoretotal1=tmp.filteredforeignerminscoretotal1,\n" +
                "    filteredforeignerq1scoretotal1=tmp.filteredforeignerq1scoretotal1,\n" +
                "    filteredforeignerq2scoretotal1=tmp.filteredforeignerq2scoretotal1,\n" +
                "    filteredforeignerq3scoretotal1=tmp.filteredforeignerq3scoretotal1,\n" +
                "    filteredforeignermeanscoretotal1=tmp.filteredforeignermeanscoretotal1,\n" +
                "    filteredforeignerstddevscoretotal1=tmp.filteredforeignerstddevscoretotal1,\n" +
                "    filteredforeignerlowscoretotal1 = tmp.filteredforeignermeanscoretotal1 - tmp.filteredstddevscoretotal1,\n" +
                "    filteredforeignerhighscoretotal1 = tmp.filteredforeignermeanscoretotal1 + tmp.filteredstddevscoretotal1,\n" +
                "\n" +
                "    filteredforeignerdetailminscoretotal1=tmp.filteredforeignerdetailminscoretotal1,\n" +
                "    filteredforeignerdetailq1scoretotal1=tmp.filteredforeignerdetailq1scoretotal1,\n" +
                "    filteredforeignerdetailq2scoretotal1=tmp.filteredforeignerdetailq2scoretotal1,\n" +
                "    filteredforeignerdetailq3scoretotal1=tmp.filteredforeignerdetailq3scoretotal1,\n" +
                "    filteredforeignerdetailmeanscoretotal1=tmp.filteredforeignerdetailmeanscoretotal1,\n" +
                "    filteredforeignerdetailstddevscoretotal1=tmp.filteredforeignerdetailstddevscoretotal1,\n" +
                "    filteredforeignerdetaillowscoretotal1 = tmp.filteredforeignerdetailmeanscoretotal1 - tmp.filteredstddevscoretotal1,\n" +
                "    filteredforeignerdetailhighscoretotal1 = tmp.filteredforeignerdetailmeanscoretotal1 + tmp.filteredstddevscoretotal1,\n" +
                "\n" +
                "    filteredinsiderminscoretotal1=tmp.filteredinsiderminscoretotal1,\n" +
                "    filteredinsiderq1scoretotal1=tmp.filteredinsiderq1scoretotal1,\n" +
                "    filteredinsiderq2scoretotal1=tmp.filteredinsiderq2scoretotal1,\n" +
                "    filteredinsiderq3scoretotal1=tmp.filteredinsiderq3scoretotal1,\n" +
                "    filteredinsidermeanscoretotal1=tmp.filteredinsidermeanscoretotal1,\n" +
                "    filteredinsiderstddevscoretotal1=tmp.filteredinsiderstddevscoretotal1,\n" +
                "    filteredinsiderlowscoretotal1 = tmp.filteredinsidermeanscoretotal1 - tmp.filteredstddevscoretotal1,\n" +
                "    filteredinsiderhighscoretotal1 = tmp.filteredinsidermeanscoretotal1 + tmp.filteredstddevscoretotal1,\n" +
                "\n" +
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
