package org.soluvas.scrape.core;

import net.sf.ehcache.CacheManager;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DecompressingHttpClient;
import org.apache.http.impl.client.cache.CacheConfig;
import org.apache.http.impl.client.cache.CachingHttpClients;
import org.apache.http.impl.client.cache.ManagedHttpCacheStorage;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.io.File;

/**
 * Created by ceefour on 7/1/15.
 */
@Configuration
@Profile("scraper")
public class ScrapeConfig {

    @Bean(destroyMethod = "close")
    public CloseableHttpClient httpClient() {
//        final CacheConfig cacheConfig = CacheConfig.custom()
//                .setMaxCacheEntries(1000)
//                .setMaxObjectSize(16 * 1024 * 1024)
//                .build();
//        final ManagedHttpCacheStorage managedCache = new ManagedHttpCacheStorage(cacheConfig);
        return CachingHttpClients.createFileBound(new File("cache/httpclient"));
    }
}
