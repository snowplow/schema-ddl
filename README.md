# Schema DDL

[![Maven Central][maven-image]][maven]
[![Build Status][build-image]][build] 
[![License][license-image]][license]
[![Coverage Status][coveralls-image]][coveralls]

Schema DDL is a set of Abstract Syntax Trees and generators for producing
various DDL (such as Redshift, Postgres or BigQuery) from JSON Schema.
The library is a core part of **[Iglu][iglu]** ecosystem and broadly used in **[Snowplow Platform][snowplow]**.
Schema DDL itself does not provide any CLI and expose only Scala API.

## Quickstart

Schema DDL is compiled against Scala 2.12 and 2.13 and available on Maven Central. In order to use it with SBT, include following module:

```scala
libraryDependencies += "com.snowplowanalytics" %% "schema-ddl" % "0.14.3"
```


## Find out more

| **[Iglu][iglu]**          | **[API Reference][api-reference]**          | **[Developer Guide][developer-guide]**          | **[Contributing][contributing]**          |
|---------------------------|---------------------------------------------|-------------------------------------------------|-------------------------------------------|
| [![i1][iglu-image]][iglu] | [![i2][api-reference-image]][api-reference] | [![i3][developer-guide-image]][developer-guide] | [![i4][contributing-image]][contributing] |


## Copyright and License

Schema DDL is copyright 2014-2021 Snowplow Analytics Ltd.

Licensed under the **[Apache License, Version 2.0][license]** (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


[maven]: https://maven-badges.herokuapp.com/maven-central/com.snowplowanalytics/schema-ddl_2.12
[maven-image]: https://maven-badges.herokuapp.com/maven-central/com.snowplowanalytics/schema-ddl_2.12/badge.svg

[license-image]: http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat
[license]: http://www.apache.org/licenses/LICENSE-2.0

[build]: https://github.com/snowplow/schema-ddl/actions?query=workflow%3A%22Test+and+deploy%22
[build-image]: https://github.com/snowplow/schema-ddl/workflows/Test%20and%20deploy/badge.svg

[coveralls]: https://coveralls.io/github/snowplow/schema-ddl?branch=master
[coveralls-image]: https://coveralls.io/repos/github/snowplow/schema-ddl/badge.svg?branch=master

[snowplow]: https://github.com/snowplow/snowplow
[self-describing]: http://snowplowanalytics.com/blog/2014/05/15/introducing-self-describing-jsons/

[developer-guide]: https://github.com/snowplow/schema-ddl/wiki/
[developer-guide-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png

[iglu]: https://docs.snowplowanalytics.com/docs/iglu/
[iglu-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png

[contributing]: https://docs.snowplowanalytics.com/docs/contributing/
[contributing-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/contributing.png

[api-reference]: https://snowplow.github.io/schema-ddl/0.14.3/com/snowplowanalytics/iglu/schemaddl/index.html
[api-reference-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
