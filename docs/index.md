Siddhi Store Redis
===================

  [![Jenkins Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-store-redis/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-store-redis/)
  [![GitHub Release](https://img.shields.io/github/release/siddhi-io/siddhi-store-redis.svg)](https://github.com/siddhi-io/siddhi-store-redis/releases)
  [![GitHub Release Date](https://img.shields.io/github/release-date/siddhi-io/siddhi-store-redis.svg)](https://github.com/siddhi-io/siddhi-store-redis/releases)
  [![GitHub Open Issues](https://img.shields.io/github/issues-raw/siddhi-io/siddhi-store-redis.svg)](https://github.com/siddhi-io/siddhi-store-redis/issues)
  [![GitHub Last Commit](https://img.shields.io/github/last-commit/siddhi-io/siddhi-store-redis.svg)](https://github.com/siddhi-io/siddhi-store-redis/commits/master)
  [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

The **siddhi-store-redis extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a> that persist and retrieve events to/from Redis.

For information on <a target="_blank" href="https://siddhi.io/">Siddhi</a> and it's features refer <a target="_blank" href="https://siddhi.io/redirect/docs.html">Siddhi Documentation</a>. 

## Download

* Versions 3.x and above with group id `io.siddhi.extension.*` from <a target="_blank" href="https://mvnrepository.com/artifact/io.siddhi.extension.store.redis/siddhi-store-redis/">here</a>.
* Versions 2.x and lower with group id `org.wso2.extension.siddhi.*` from <a target="_blank" href="https://mvnrepository.com/artifact/org.wso2.extension.siddhi.store.redis/siddhi-store-redis">here</a>.

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://siddhi-io.github.io/siddhi-store-redis/api/3.1.2">3.1.2</a>.

## Features

* <a target="_blank" href="https://siddhi-io.github.io/siddhi-store-redis/api/3.1.2/#redis-store">redis</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#store">Store</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">This extension assigns data source and connection instructions to event tables. It also implements read write operations on connected datasource. This extension only can be used to read the data which persisted using the same extension since unique implementation has been used to map the relational data in to redis's key and value representation</p></p></div>

## Dependencies 

This extension depends on Jedis, redis client. Please download redis client jar (2.9.0) use it.
Tested/supported with Redis cloud instance version 4.x.x .

## Installation

For installing this extension on various siddhi execution environments refer Siddhi documentation section on <a target="_blank" href="https://siddhi.io/redirect/add-extensions.html">adding extensions</a>.

## Support and Contribution

* We encourage users to ask questions and get support via <a target="_blank" href="https://stackoverflow.com/questions/tagged/siddhi">StackOverflow</a>, make sure to add the `siddhi` tag to the issue for better response.

* If you find any issues related to the extension please report them on <a target="_blank" href="https://github.com/siddhi-io/siddhi-execution-string/issues">the issue tracker</a>.

* For production support and other contribution related information refer <a target="_blank" href="https://siddhi.io/community/">Siddhi Community</a> documentation.

