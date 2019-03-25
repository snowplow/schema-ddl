#!/bin/bash

set -e
tag=$1

limit=30    # Timeout to extend Travis build time (from 10 mins)

mkdir ~/.bintray/
FILE=$HOME/.bintray/.credentials
cat <<EOF >$FILE
realm = Bintray API Realm
host = api.bintray.com
user = $BINTRAY_SNOWPLOW_MAVEN_USER
password = $BINTRAY_SNOWPLOW_MAVEN_API_KEY
EOF

cd "${TRAVIS_BUILD_DIR}"

project_version=$(sbt version -Dsbt.log.noformat=true | tail -n 1 | perl -ne 'print $1 if /(\d+\.\d+[^\r\n]*)/')

function travis_wait {
  minutes=0
  while kill -0 $! >/dev/null 2>&1; do
    echo -n -e " \b" # never leave evidences!
  
    if [ $minutes == $limit ]; then
      break;
    fi
  
    minutes=$((minutes+1))
  
    sleep 60
  done
}

if [ "${project_version}" == "${tag}" ]; then
    # local publish only dependency, scala-core
    # universal publish schema-ddl
    cd "${TRAVIS_BUILD_DIR}"
    echo "DEPLOY: testing schema-ddl..."
    sbt +test --warn
    echo "DEPLOY: publishing schema-ddl..."
    sbt +publish
    echo "DEPLOY: publishing schema-ddl to Maven Central..."
    sbt +bintraySyncMavenCentral &
    travis_wait
    echo "DEPLOY: Schema DDL deployed..."

else
    echo "Tag version '${tag}' doesn't match version in scala project ('${project_version}'). Aborting!"
    exit 1
fi
