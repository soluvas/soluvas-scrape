package org.soluvas.scrape.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;
import org.joda.time.DateTime;
import org.soluvas.json.JsonUtils;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import javax.annotation.Nullable;
import java.util.EnumSet;
import java.util.Set;

/**
 * Created by ceefour on 7/1/15.
 */
@Service
@Profile("scraper")
public class Scraper {

    static {
        Configuration.setDefaults(new Configuration.Defaults() {
            private final JsonProvider jsonProvider = new JacksonJsonNodeJsonProvider(JsonUtils.mapper);
            private final MappingProvider mappingProvider = new JacksonMappingProvider(JsonUtils.mapper);

            @Override
            public JsonProvider jsonProvider() {
                return jsonProvider;
            }

            @Override
            public MappingProvider mappingProvider() {
                return mappingProvider;
            }

            @Override
            public Set<Option> options() {
                return EnumSet.noneOf(Option.class);
            }
        });
    }

    public ScrapeData scrape(ScrapeTemplate template,
                             FetchData fetchData) {
        final ScrapeData scrapeData = new ScrapeData();
        scrapeData.setCreationTime(new DateTime());

        for (final CollectionDef collDef : template.getCollections()) {
            final CollectionData collData = new CollectionData();
            collData.setDefinition(collDef);

            final ArrayNode collArray;
            switch (collDef.getSource()) {
                case ROOT:
                    collArray = (ArrayNode) fetchData.getJsonRpcResult().getResult();
                    break;
                case PATH:
                    collArray = JsonPath.read(fetchData.getJsonRpcResult().getResult(), collDef.getSourceExpression());
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported source: " + collDef.getSource());
            }

            final ImmutableList<EntityData> entities = FluentIterable.from(collArray)
                    .transform(entityObj -> {
                        final EntityData entity = new EntityData();
                        entity.setId(entityObj.get(collDef.getIdProperty()).asText());
                        entity.setName(entityObj.get(collDef.getNameProperty()).asText());

                        for (final PropertyDef propDef : collDef.getProperties()) {
                            final PropertyData propData = new PropertyData();
                            propData.setDefinition(propDef);
                            final PropertyValue propValue = new PropertyValue();

                            if (propDef.getSource() == null || propDef.getSource() == PropertySource.RESPONSE) {
                                switch (propDef.getKind()) {
                                    case BOOLEAN:
                                        propData.getValues().add(new PropertyValue(entityObj.get(propDef.getId()).asBoolean()));
                                        break;
                                    case ENUMERATION:
                                        propData.getValues().add(new PropertyValue(entityObj.get(propDef.getId()).asText()));
                                        break;
                                    case INTEGER:
                                        propData.getValues().add(new PropertyValue(entityObj.get(propDef.getId()).asInt()));
                                        break;
                                    case DOUBLE:
                                        propData.getValues().add(new PropertyValue(entityObj.get(propDef.getId()).asDouble()));
                                        break;
                                    case STRING:
                                        propData.getValues().add(new PropertyValue(entityObj.get(propDef.getId()).asText()));
                                        break;
                                    case JSON_OBJECT:
                                        if (propDef.getCardinality() == Cardinality.MULTIPLE) {
                                            entityObj.get(propDef.getId()).forEach(valueNode ->
                                                            propData.getValues().add(new PropertyValue(valueNode))
                                            );
                                        } else {
                                            @Nullable
                                            final JsonNode objectOrNullNode = entityObj.get(propDef.getId());
                                            if (objectOrNullNode != null && !objectOrNullNode.isNull()) {
                                                propData.getValues().add(new PropertyValue((ObjectNode) objectOrNullNode));
                                            }
                                        }
                                        break;
                                    default:
                                        throw new IllegalArgumentException("Unsupported property kind: " + propDef.getKind());
                                }
                            } else if (propDef.getSource() == PropertySource.REQUEST_PARAMETER) {
                                propData.getValues().add(new PropertyValue(fetchData.getRequestParams().get(propDef.getParameterId())));
                            } else {
                                throw new IllegalArgumentException("Unsupported property source: " + propDef.getSource());
                            }

                            entity.getProperties().add(propData);
                        }

                        return entity;
                    }).toList();
            collData.getEntities().addAll(entities);

            scrapeData.getCollections().add(collData);
        }

        return scrapeData;
    }
}
