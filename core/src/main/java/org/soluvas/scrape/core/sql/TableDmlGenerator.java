package org.soluvas.scrape.core.sql;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.soluvas.scrape.core.*;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSourceUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by ceefour on 7/1/15.
 */
@Profile("sql")
@Service
public class TableDmlGenerator {

    private static final Logger log = LoggerFactory.getLogger(TableDmlGenerator.class);

    static class UpsertSql {
        String updateSql;
        String insertSql;

        public UpsertSql(String updateSql, String insertSql) {
            this.updateSql = updateSql;
            this.insertSql = insertSql;
        }

        public String getUpdateSql() {
            return updateSql;
        }

        public String getInsertSql() {
            return insertSql;
        }

        @Override
        public String toString() {
            return "UpsertSql{" +
                    "updateSql='" + updateSql + '\'' +
                    ", insertSql='" + insertSql + '\'' +
                    '}';
        }
    }

    /**
     * UPDATE table SET field='C', field2='Z' WHERE id=3;
     * INSERT INTO table (id, field, field2)
     * SELECT 3, 'C', 'Z'
     * WHERE NOT EXISTS (SELECT 1 FROM table WHERE id=3);
     * @param schemaName
     * @param template
     * @return
     * @throws IOException
     */
    public UpsertSql generateUpsert(String schemaName, ScrapeTemplate template,
                                 String collId) {
        final CollectionDef collDef = template.getCollections().stream().filter(it -> collId.equals(collId)).findAny().get();
        final List<PropertyDef> otherProps = collDef.getProperties().stream()
                .filter(it -> !it.getId().equals(collDef.getIdProperty()) && !it.getId().equals(collDef.getNameProperty()))
                .collect(Collectors.toList());
        final LinkedHashSet<String> settedColumns = new LinkedHashSet<>();
        settedColumns.add("name");
        settedColumns.addAll(otherProps.stream().map(PropertyDef::getId).collect(Collectors.toList()));
        final LinkedHashSet<String> settedParams = new LinkedHashSet<>();
        settedParams.add("name=:name");
        settedParams.addAll(otherProps.stream()
                .map(it -> {
                    if (it.getCardinality() == Cardinality.MULTIPLE && it.getKind() == PropertyKind.JSON_OBJECT) {
                        return it.getId() + "=ARRAY[ :" + it.getId() + " ]::json[]";
                    } else if (it.getKind() == PropertyKind.JSON_OBJECT) {
                        return it.getId() + "=:" + it.getId() + "::json";
                    } else if (it.getCardinality() == Cardinality.MULTIPLE) {
                        return it.getId() + "=ARRAY[ :" + it.getId() + " ]";
                    } else {
                        return it.getId() + "=:" + it.getId();
                    }
                }).collect(Collectors.toList()));
        final LinkedHashSet<String> settedValues = new LinkedHashSet<>();
        settedValues.add(":name");
        settedValues.addAll(otherProps.stream()
                .map(it -> {
                    if (it.getCardinality() == Cardinality.MULTIPLE && it.getKind() == PropertyKind.JSON_OBJECT) {
                        return "ARRAY[ :" + it.getId() + " ]::json[]";
                    } else if (it.getKind() == PropertyKind.JSON_OBJECT) {
                        return ":" + it.getId() + "::json";
                    } else if (it.getCardinality() == Cardinality.MULTIPLE) {
                        return "ARRAY[ :" + it.getId() + " ]";
                    } else {
                        return ":" + it.getId();
                    }
                }).collect(Collectors.toList()));

        final String updateSql = "UPDATE " + schemaName + "." + collDef.getId() + "\n  SET\n    "
                + Joiner.on(",\n    ").join(settedParams)
                + "\n  WHERE id=:id;";
        final String insertSql = "INSERT INTO " + schemaName + "." + collDef.getId()
                + " (id, " + Joiner.on(", ").join(settedColumns) + ")\n"
                + "  SELECT :id, " + Joiner.on(", ").join(settedValues) + "\n"
                + "  WHERE NOT EXISTS (SELECT 1 FROM " + schemaName + "." + collDef.getId() + " WHERE id=:id);";
        return new UpsertSql(updateSql, insertSql);
    }

    public void upsert(String schemaName, ScrapeTemplate template,
                       ScrapeData scrapeData, DataSource dataSource,
                       PlatformTransactionManager txMgr) {
        final NamedParameterJdbcTemplate jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        new TransactionTemplate(txMgr).execute(tx -> {
            for (final CollectionData collData : scrapeData.getCollections()) {
                final UpsertSql upsertSql = generateUpsert(schemaName, template, collData.getDefinition().getId());
                log.info("Upsert for {}: {}", collData.getDefinition().getId(), upsertSql);
                for (final EntityData entity : collData.getEntities()) {
                    final LinkedHashMap<String, Object> params = new LinkedHashMap<>();
                    params.put("id", entity.getId());
                    params.put("name", entity.getName());
                    entity.getProperties().forEach(prop -> {
                        final Stream<Object> sqlValueStream = prop.getValues().stream()
                                .map(value -> value.getValue() instanceof JsonNode ? value.getJsonObject().toString() : value.getValue());
                        if (prop.getDefinition().getCardinality() == Cardinality.MULTIPLE) {
                            params.put(prop.getDefinition().getId(), sqlValueStream.collect(Collectors.toList()));
                        } else {
                            params.put(prop.getDefinition().getId(), sqlValueStream.findFirst().orElse(null));
                        }
                    });
                    log.debug("Upserting {} {} ({})...", collData.getDefinition().getId(), entity.getId(), entity.getName());
                    jdbcTemplate.update(upsertSql.updateSql, params);
                    jdbcTemplate.update(upsertSql.insertSql, params);
                }
            }
            return null;
        });
    }

}
