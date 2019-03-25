#!/bin/bash

set -e

cd "${TRAVIS_BUILD_DIR}/0-common/schema-ddl"
sbt +publishLocal --warn
