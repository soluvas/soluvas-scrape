package org.soluvas.scrape.core.sql;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import org.soluvas.scrape.core.Cardinality;
import org.soluvas.scrape.core.PropertyDef;
import org.soluvas.scrape.core.PropertyKind;
import org.soluvas.scrape.core.ScrapeTemplate;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ceefour on 7/1/15.
 */
@Profile("sql")
@Service
public class TableDdlGenerator {

    static class Column {
        String name;
        String type;

        public Column(String name, String type) {
            this.name = name;
            this.type = type;
        }
    }

    static class Table {
        String schemaName;
        String tableName;
        String idType;
        List<Column> columns = new ArrayList<>();

        public Table(String schemaName, String tableName, String idType) {
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.idType = idType;
        }
    }

    static class CreateTableData {
        List<Table> tables = new ArrayList<>();
    }

    public String kindToSqlType(PropertyKind kind, Cardinality cardinality) {
        final String cardinalityStr = cardinality == Cardinality.MULTIPLE ? "[]" : "";
        switch (kind) {
            case BOOLEAN:
                return "boolean" + cardinalityStr;
            case ENUMERATION:
                return "varchar(255)" + cardinalityStr;
            case INTEGER:
                return "integer" + cardinalityStr;
            case DOUBLE:
                return "double precision" + cardinalityStr;
            case JSON_OBJECT:
                return "json" + cardinalityStr;
            case STRING:
                return "varchar(255)" + cardinalityStr;
            case TEXT:
                return "text" + cardinalityStr;
            default:
                throw new IllegalArgumentException("Unsupported kind: " + kind);
        }
    }

    public String generateCreateTable(String schemaName, ScrapeTemplate template) throws IOException {
        final CreateTableData createTableData = new CreateTableData();
        template.getCollections().forEach(collDef -> {
            final PropertyDef idProp = collDef.getProperties().stream()
                    .filter(it -> it.getId().equals(collDef.getIdProperty())).findAny().get();
//            final PropertyDef nameProp = collDef.getProperties().stream()
//                    .filter(it -> it.getId().equals(collDef.getNameProperty())).findAny().get();
            final Table table = new Table(schemaName, collDef.getId(), kindToSqlType(idProp.getKind(), idProp.getCardinality()));
            for (final PropertyDef propDef : collDef.getProperties()) {
                if (collDef.getIdProperty().equals(propDef.getId())) {
                    continue;
                }
                if (collDef.getNameProperty().equals(propDef.getId())) {
                    continue;
                }
                table.columns.add(new Column(propDef.getId(), kindToSqlType(propDef.getKind(), Cardinality.SINGLE)));
            }
            createTableData.tables.add(table);
        });

        final DefaultMustacheFactory mustacheFactory = new DefaultMustacheFactory("org/soluvas/scrape/core/sql");
        final Mustache mustache = mustacheFactory.compile("create_table.sql.mustache");
        try (StringWriter sw = new StringWriter()) {
            mustache.execute(sw, createTableData);
            return sw.toString();
        }
    }
}
