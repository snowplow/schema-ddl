/*
 * Copyright (c) 2014-2023 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.iglu.schemaddl


import java.util.regex.Pattern

/**
 * Utilities for manipulating Strings
 */
object StringUtils {

  private val splitAcronyms = Pattern.compile("([A-Z]+)([A-Z][a-z])")

  private val splitCamelCase = Pattern.compile("([a-z\\d])([A-Z])")

  /**
   * Transforms CamelCase string into snake_case
   * Also replaces all hyphens with underscores
   */
  val snakeCase: String => String = str =>
    splitCamelCase.matcher(splitAcronyms.matcher(str).replaceAll("$1_$2"))
      .replaceAll("$1_$2")
      .replace('-', '_')
      .toLowerCase

}
