[![Build Status](https://github.com/sixhours-team/memcached-spring-boot/actions/workflows/build.yml/badge.svg)](https://github.com/sixhours-team/memcached-spring-boot/actions/workflows/build.yml)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=io.sixhours%3Amemcached-spring-boot&metric=coverage)](https://sonarcloud.io/summary/new_code?id=io.sixhours%3Amemcached-spring-boot)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=io.sixhours%3Amemcached-spring-boot&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=io.sixhours%3Amemcached-spring-boot)
[![Join the chat at gitter.im/six-hours/memcached-spring-boot](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/six-hours/memcached-spring-boot?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)

# Memcached Spring Boot

Library that provides support for auto-configuration of Memcached cache in a Spring Boot application.

It provides implementation for the [Spring Cache Abstraction](https://docs.spring.io/spring/docs/5.2.3.RELEASE/spring-framework-reference/integration.html#cache), backed by the [Xmemcached](https://github.com/killme2008/xmemcached).
Supports cache eviction per key, as well as clearing out of the entire cache region. Binaries are available from **Maven Central**.

## Usage

To plug-in Memcached cache in your application follow the steps below:

1. Include library as a Gradle or Maven compile dependency:
   * **Gradle**

      ```groovy
      implementation('io.sixhours:memcached-spring-boot-starter:2.4.4')
      ```
   * **Maven**

      ```xml
      <dependency>
          <groupId>io.sixhours</groupId>
          <artifactId>memcached-spring-boot-starter</artifactId>
          <version>2.4.4</version>
      </dependency>
      ```
  
    As the default implementation is backed by [Xmemcached](https://github.com/killme2008/xmemcached) if there is a requirement to use [Spymemcached](https://github.com/awslabs/aws-elasticache-cluster-client-memcached-for-java) (i.e. its forked AWS version) following configuation should be applied:
  
   * **Gradle**

        ```groovy
        implementation('io.sixhours:memcached-spring-boot-starter:2.4.4') {
          exclude group: 'com.googlecode.xmemcached', module: 'xmemcached'
        }
        implementation('com.amazonaws:elasticache-java-cluster-client:1.1.2')
        ```
   * **Maven**

        ```xml
        <dependency>
            <groupId>io.sixhours</groupId>
            <artifactId>memcached-spring-boot-starter</artifactId>
            <version>2.4.4</version>
            <exclusions>
                <exclusion>
                    <groupId>com.googlecode.xmemcached</groupId>
                    <artifactId>xmemcached</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
        <dependency>
            <groupId>com.amazonaws</groupId>
            <artifactId>elasticache-java-cluster-client</artifactId>
            <version>1.1.2</version>
        </dependency>
        ```

  * Snapshot repository

    If you want to use `SNAPSHOT` versions, add the snapshot-repo `https://oss.sonatype.org/content/repositories/snapshots` as shown in the [example](https://github.com/sixhours-team/spring-boot-memcached-demo-java/blob/master/build.gradle#L16).

2. Configure `Memcached` key-value store in your properties file (`application.yml`).

    **Example**

    To manually connect to one or more cache servers (nodes), specify comma-separated list of hostname:port with the `static` provider:

    ```yaml
     memcached.cache:
       servers: example1.com:11211,example2.com:11211
       provider: static
       # default expiration set to '1d' (1 day i.e. '86400' seconds) and custom ones for cache_name1 and cache_name2
       expiration: 1d
       expiration-per-cache:
         cache_name1: 1h
         cache_name2: 30h
       metrics-cache-names: cache_name1, cache_name2
     ```

    To connect to a cluster with AWS [Auto Discovery](http://docs.aws.amazon.com/AmazonElastiCache/latest/UserGuide/AutoDiscovery.html), specify
    cluster configuration endpoint in `memcached.cache.servers` property with the `aws` provider:

    ```yaml
    memcached.cache:
        servers: mycluster.example.com:11211
        provider: aws
        expiration: 86400 # default expiration set to '86400' seconds i.e. 1 day
    ```

    To connect to a cluster within Google App Engine memcached service, it is sufficient to specify
    the configuration property for provider with value `appengine`:

    ```yaml
    memcached.cache:
        provider: appengine
        expiration: 86400 # default expiration set to '86400' seconds i.e. 1 day
    ```

3. Enable caching support by adding `@EnableCaching` annotation to one of your `@Configuration` classes.

    ```java
    import org.springframework.boot.SpringApplication;
    import org.springframework.boot.autoconfigure.SpringBootApplication;
    import org.springframework.cache.annotation.EnableCaching;

    @SpringBootApplication
    @EnableCaching
    public class Application {

    	public static void main(String[] args) {
    		SpringApplication.run(Application.class, args);
    	}
    }
    ```

    Now you can add caching to an operation of your service:

    ```java
    import org.springframework.cache.annotation.Cacheable;
    import org.springframework.stereotype.Component;

    @Component
    public class BookService {

        @Cacheable("books")
        public Book findByTitle(String title) {
            // ...
        }

    }
    ```

For further details on using the Memcached cache in a Spring Boot application please look at the [demo](https://github.com/sixhours-team/spring-boot-memcached-demo-java) project.

## Properties

Properties can be set in your `application.yml` file or as a command line properties. Below is the
full list of supported properties:

```yaml
# MEMCACHED CACHE
memcached.cache.servers: # Comma-separated list of hostname:port for memcached servers (default "localhost:11211")
memcached.cache.provider: # Memcached server provider (use one of following: "static", "aws" or "appengine"). Default provider is "static". Use "aws" for AWS node auto discovery, or "appengine" if running on Google Cloud Platform.
memcached.cache.expiration: # Default cache expiration (defaults to "0", meaning that cache will never expire). If duration unit is not specified, seconds will be used by default.
memcached.cache.expiration-per-cache.cacheName: # Set expiration for cache with given name. Overrides `memcached.cache.expiration` for the given cache. To set expiration value for cache named "cacheName" {cache_name}:{number} e.g. "authors: 3600" or "authors: 1h". If duration unit is not specified, seconds will be used by default.
memcached.cache.prefix: # Cache key prefix (default "memcached:spring-boot")
memcached.cache.protocol: # Memcached client protocol. Supports "text" and "binary" protocols (default is "text" protocol)
memcached.cache.operation-timeout: # Memcached client operation timeout (default "2500 milliseconds"). If unit not specified, milliseconds will be used.
memcached.cache.hash-strategy: # Memcached client hash strategy for distribution of data between servers. Supports "standard" (array based : "hash(key) mod server_count"), "libmemcached" (consistent hash), "ketama" (consistent hash), "php" (make easier to share data with PHP based clients), "election", "roundrobin", "random". Default is "standard".
memcached.cache.servers-refresh-interval: # Interval in milliseconds that refreshes the list of cache node hostnames and IP addresses for AWS ElastiCache. The default is 60000 milliseconds.
memcached.cache.metrics-cache-names: # Comma-separated list of cache names for which metrics will be collected.
memcached.cache.disabled-cache-names: # Comma-separated list of cache names for which caching will be disabled. The main purpose of this property is to disable caching for debugging purposes.    
```

All of the values have sensible defaults and are bound to [MemcachedCacheProperties](https://github.com/sixhours-team/memcached-spring-boot/blob/master/memcached-spring-boot-autoconfigure/src/main/java/io/sixhours/memcached/cache/MemcachedCacheProperties.java) class.

Duration properties such as `expiration` and `expiration-per-cache` by default are using unit of seconds if no unit is specified. For `operation-timeout` property unit of milliseconds is the default one.

E.g. to specify an `expiration` of 30 seconds, `30`, `PT30S` (ISO-8601 format) and `30s` are all equivalent. An `operation-timeout` of 500ms can be specified in any of the following form: `500`, `PT0.5S` and `500ms`.

Supported units are:

* `ns` for nanoseconds

* `us` for microseconds

* `ms` for milliseconds

* `s` for seconds

* `m` for minutes

* `h` for hours

* `d` for days

**Notice:**
>If different applications are sharing the same Memcached server, make sure to specify unique cache `prefix` for each application
in order to avoid cache conflicts.

## Build

In order to build the project you will have to have [Java 1.8+](http://www.oracle.com/technetwork/java/javase/downloads/index.html) and [Docker](https://www.docker.com/get-docker) installed.
To build the project invoke the following command:

    ./gradlew clean build

To install the modules in the local Maven repository:

    ./gradlew clean build publishToMavenLocal

## License

Memcached Spring Boot is an Open Source software released under the [Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0.html).
