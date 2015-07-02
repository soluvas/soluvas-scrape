# soluvas-scrape
Declarative web site scraping library/framework in Java. Uses Jaunt.

## Underlying Library

1. [Jaunt](http://jaunt-api.com/). Primary engine.
2. TBD: [Ghost Driver](https://github.com/detro/ghostdriver) + [PhantomJS](http://phantomjs.org/) for advanced use cases requiring JavaScript support.
3. TBD: [jSoup](http://jsoup.org). For simpler use cases, jSoup may be enough.

## Planned Output Formats and Databases

1. **JSON** (TODO)
2. **PostgreSQL** (TODO)
3. **CSV** (TODO)
4. **MongoDB** (TODO)
5. **Parquet** (TODO). So you can query with Hive/Pig.
6. **HBase** (TODO). So you can query with Hive/Pig.
7. **XLSX** (TODO)
8. **ODS** (TODO)

## Concepts

1. **Collection**. Contains list of entities, all have same schema of properties.
2. **Property**. Part of a schema. All entities in a collection have the same property definitions, but not all are filled.
3. **Entity**. A single element in a Collection.

## Planned Features

1. **HTTP-level Caching with Apache HttpClient + ManagedHttpCache**. Effective only for GET requests, and has no effect
    for POST/JSON-RPC requests.

2. **Application-level Caching with Spring Caching + EhCache alternative**. Will work for
    POST/JSON-RPC/social media APIs, but cannot use EhCache due to Enterprise-only `localRestartable`.
    [GridGain/Apache Ignite](https://ignite.incubator.apache.org/) seem to be best here, not to mention its
    [Spark/Hadoop integration](https://apacheignite.readme.io/v1.2/docs/overview).

2. **Historical/Time Series Data support** (TODO). So you can track, e.g. estimated passing grade for several schools over time.
    Two possible approaches: materialized (write-first) and embedded (calculate-on-query).
    With materialized, during data collection, it also writes time series data in different tables.
    With embedded, historical data is saved in the same table as the original data (which the original data schema
    needs to be modified), then they're processed during reporting/query time.
    We'll be using materialized as it is more scalable and can implemented in any database, e.g. PostgreSQL, MongoDB,
    Cassandra, HBase, even file-based formats like Parquet.
    
    However:
    
    * it's hard to go back and change the time series data if there's any mistake in the implementation,
      but over time this should happen less.
    * if you need to define a new time series data, this can only apply to future data points, not past data points.

3. **Generate JPA Entity class from ScrapeTemplate** (TODO).
4. **Generate Liquibase from ScrapeTemplate** (TODO).
5. **Generate R ggplot2 charts and histograms** (TODO).
6. **Generate R Shiny charts and histograms** (TODO).
7. **Generate interactive D3 charts and histograms** (TODO).
8. **Generate interactive Tableau visualization definition** (TODO).
9. **Save raw requests & server responses to JSON or HTML files, with request metadata** (TODO).
10. **Save raw requests & server responses to Postman Catalog** (TODO).

## Supported Protocols/Input Formats

1. **JSON-RPC over HTTP(S)** (TODO).
2. **HTML over HTTP(S)** (TODO).
3. **Twitter API** (TODO).
4. **Foursquare API** (TODO).
5. **Facebook API** (TODO).

## Sample Data

For Hendy, sample data is at: `~/Dropbox/Hendy_Projects/soluvas-scrape/sample/ppdb`
