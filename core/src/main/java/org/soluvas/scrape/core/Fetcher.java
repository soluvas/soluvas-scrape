package org.soluvas.scrape.core;

import org.apache.camel.Body;
import org.apache.camel.Handler;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.cache.HttpCacheContext;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.soluvas.json.JsonUtils;
import org.soluvas.json.LowerEnumSerializer;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Fethes raw content from remote systems.
 * Multi-threaded, cacheable.
 * Created by ceefour on 7/1/15.
 */
@Service
@Profile("scraper")
public class Fetcher {

    private static final Logger log = LoggerFactory.getLogger(Fetcher.class);

    @Inject
    private CloseableHttpClient httpClient;

    @Handler
    public FetchData fetch(ScrapeTemplate template,
                      Map<String, Object> actualParams) {
        LowerEnumSerializer.LOWER = false;
        final String uri = template.getUri();
        log.info("Scraping {} {} {} ...", template.getProtocol(), template.getProtocolVersion(),
                template.getUri());
        try {
            // FIXME: Use Retryer (with limit) when: Caused by: java.net.SocketException: Connection reset
            // at java.net.SocketInputStream.read(SocketInputStream.java:209)

            switch (template.getProtocol()) {
                case JSONRPC:
                    return fetchJsonRpc(uri, template, actualParams);
                case HTTP:
                    throw new UnsupportedOperationException("no HTTP support yet");
                default:
                    throw new UnsupportedOperationException("Unsupported protocol: " + template.getProtocol());
            }
        } catch (Exception e) {
            throw new ScrapeException(e, "Cannot fetch %s", uri);
        }
    }

    protected FetchData fetchJsonRpc(String uri, ScrapeTemplate template, Map<String, Object> actualParams) throws IOException {
        final FetchData fetchData = new FetchData();
        fetchData.setUri(uri);
        fetchData.setProtocol(template.getProtocol());
        fetchData.setProtocolVersion(template.getProtocolVersion());
        fetchData.getRequestParams().putAll(actualParams);

        final HttpCacheContext context = HttpCacheContext.create();
        final HttpPost postReq = new HttpPost(uri);
        final JsonRpc2MethodCall methodCall = new JsonRpc2MethodCall();
        methodCall.setMethod(template.getRpcMethod());
        methodCall.getParams().putAll(actualParams);
        postReq.setEntity(
                new StringEntity(JsonUtils.mapper.writeValueAsString(methodCall), ContentType.APPLICATION_JSON)
        );
        log.info("Fetching {} {} {} {} {} ...",
                template.getProtocol(), template.getProtocolVersion(), uri, template.getRpcMethod(), actualParams);
        try (CloseableHttpResponse resp = httpClient.execute(postReq, context)) {
            log.info("Received {} {} {} bytes", context.getCacheResponseStatus(),
                    resp.getEntity().getContentType(), resp.getEntity().getContentLength());
            final String entityBody = IOUtils.toString(resp.getEntity().getContent(), StandardCharsets.UTF_8);
            try {
                final JsonRpc2MethodResult methodResult = JsonUtils.mapper.readValue(entityBody, JsonRpc2MethodResult.class);
                log.info("JSON-RPC Method result: {}", methodResult);
                fetchData.setJsonRpcResult(methodResult);
                if (methodResult.getError() != null) {
                    throw new ScrapeException(methodResult.getError(), "Error fetching %s: %s", uri, methodResult.getError());
                }
                return fetchData;
            } catch (Exception e) {
                log.error("Error converting to JSON: " + entityBody, e);
                throw new ScrapeException("Error converting to JSON (see previous log)", e);
            }
        }
    }

}
