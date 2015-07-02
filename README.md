# Soluvas Scrape

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
    Three possible approaches: materialized (write-first), embedded (calculate-on-query), and materialized+some embedded.
    With materialized, during data collection, it also writes time series data in different tables.
    
    However:
    
    * it's hard to go back and change the time series data if there's any mistake in the implementation,
      but over time this should happen less.
    * if you need to define a new time series data, this can only apply to future data points, not past data points.

    With embedded, historical data is saved in the same table as the original data (which the original data schema
    needs to be modified), then they're processed during reporting/query time.
    We'll be using materialized+some embedded as it is more scalable and can implemented in any database, e.g. PostgreSQL, MongoDB,
    Cassandra, HBase, even file-based formats like Parquet.
    By "some embedded", we also store a list of essential foreign key in the materialized table, 
    like `registration_id` for each `option_id`+`snapshottime` so we can refer to this later on.
    
3. **Generate JPA Entity class from ScrapeTemplate** (TODO).
4. **Generate Liquibase from ScrapeTemplate** (TODO).
5. **Generate R ggplot2 charts and histograms** (TODO).
6. **Generate R Shiny charts and histograms** (TODO).
7. **Generate interactive D3 charts and histograms** (TODO).
8. **Generate interactive Tableau visualization definition** (TODO).
9. **Save raw requests & server responses to JSON or HTML files, with request metadata** (TODO).
10. **Save raw requests & server responses to Postman Catalog** (TODO).
11. **Snapshot important lists for historical/time series information.**
    For example, save `registration_id[]` (array) keyed by `option_id` and `snapshottime`.
    While main purpose of each 

## Supported Protocols/Input Formats

1. **JSON-RPC over HTTP(S)**.
2. **HTML over HTTP(S)** (TODO).
3. **Twitter API** (TODO).
4. **Foursquare API** (TODO).
5. **Facebook API** (TODO).
6. **Atom/RSS Feed** (TODO).

## Database Storage

1. **Integrated User/Organization Workspace**. All workspace tables are stored inside the app database, inside the tenant's schema,
    with appropriate prefixes for namespacing (i.e. `p_` for person and `o_` for organization).
    The schema of all workspace tables are fully managed by app, and is considered responsibility of the app.
    Currently stored in PostgreSQL for storage efficiency and speed of indexing/aggregate reports,
    but maybe MongoDB or Cassandra can be good too.

## Sample Data

For Hendy, sample data is at: `~/Dropbox/Hendy_Projects/soluvas-scrape/sample/ppdb`

## Related Projects

* [**Soluvas Analytics**](https://github.com/soluvas/soluvas-analytics). After getting those data,
    present them in a pleasing and engaging way.
* [**Soluvas ETL**](https://github.com/soluvas/soluvas-etl). Integrate incoming scraped data with other systems,
    post or sync with social media.
* [**Soluvas AI**](https://github.com/soluvas/soluvas-ai). Add some intelligence to fill missing information
    or to predict new ones.
* [**Soluvas Buzz**](https://github.com/soluvas/soluvas-buzz). Run social media, email, and online marketing campaigns
    with scraped data.
* **Soluvas Publisher** will be superseded by Soluvas Scrape + Soluvas ETL/Soluvas Buzz.
