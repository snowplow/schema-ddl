# Schema DDL

[![Maven Central][maven-badge]][maven-link]
[![Build Status][travis-image]][travis] 
[![License][license-image]][license]

Schema DDL is a set of Abstract Syntax Trees and generators for producing various DDL and Schema formats.
It's tightly coupled with other tools from **[Snowplow Platform][snowplow]** like
**[Iglu][iglu]** and **[Self-describing JSON][self-describing]**.

Schema DDL itself does not provide any CLI and expose only Scala API.

## Quickstart

Schema DDL is compiled against Scala 2.12 and availble on Maven Central. In order to use it with SBT, include following module:

```scala
libraryDependencies += "com.snowplowanalytics" %% "schema-ddl" % "0.12.0"
```


## Find out more

| **[Technical Docs][techdocs]**     | **[Setup Guide][setup]**     | **[Roadmap][roadmap]**           | **[Contributing][contributing]**           |
|-------------------------------------|-------------------------------|-----------------------------------|---------------------------------------------|
| [![i1][techdocs-image]][techdocs] | [![i2][setup-image]][setup] | [![i3][roadmap-image]][roadmap] | [![i4][contributing-image]][contributing] |


## Copyright and License

Schema DDL is copyright 2014-2020 Snowplow Analytics Ltd.

Licensed under the **[Apache License, Version 2.0][license]** (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


[license-image]: http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat
[license]: http://www.apache.org/licenses/LICENSE-2.0

[maven-badge]: https://maven-badges.herokuapp.com/maven-central/com.snowplowanalytics/schema-ddl_2.12/badge.svg
[maven-link]: https://maven-badges.herokuapp.com/maven-central/com.snowplowanalytics/schema-ddl_2.12

[travis]: https://travis-ci.org/snowplow-incubator/schema-ddl
[travis-image]: https://travis-ci.org/snowplow-incubator/schema-ddl.png?branch=master

[snowplow]: https://github.com/snowplow/snowplow
[iglu]: https://github.com/snowplow/iglu
[self-describing]: http://snowplowanalytics.com/blog/2014/05/15/introducing-self-describing-jsons/

[techdocs]: https://github.com/snowplow/iglu/wiki/
[roadmap]: https://github.com/snowplow/iglu/wiki/Product-roadmap
[setup]: https://github.com/snowplow/iglu/wiki/
[contributing]: https://github.com/snowplow/iglu/wiki/Contributing

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
[contributing-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/contributing.png
