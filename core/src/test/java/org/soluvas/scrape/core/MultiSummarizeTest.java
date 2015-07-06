package org.soluvas.scrape.core;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.soluvas.scrape.core.cli.MultiScrapeThenSummarize;
import org.springframework.boot.test.ConfigFileApplicationContextInitializer;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.inject.Inject;
import java.beans.PropertyVetoException;
import java.io.IOException;

/**
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(initializers = ConfigFileApplicationContextInitializer.class,
    classes = MultiScrapeThenSummarize.Config.class)
@ActiveProfiles({"sql", "scraper", "summarizer"})
public class MultiSummarizeTest {

    private static final Logger log = LoggerFactory.getLogger(MultiSummarizeTest.class);

    @Inject
    private MultiScrapeThenSummarize multiScrapeThenSummarize;

    @Test
    public void multiScrapeThenSummarizeFromSchoolOptions() throws IOException, PropertyVetoException {
        multiScrapeThenSummarize.multiScrapeThenSummarizeFromSchoolOptions();
    }

    public void periodicMultiScrapeThenSummarizeFromSchoolOptions() throws IOException, PropertyVetoException, InterruptedException {
        multiScrapeThenSummarize.periodicMultiScrapeThenSummarizeFromSchoolOptions();
    }

}
