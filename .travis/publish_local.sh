#!/bin/bash

set -e

cd "${TRAVIS_BUILD_DIR}"
sbt +publishLocal --warn
