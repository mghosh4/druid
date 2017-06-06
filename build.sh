#!/bin/bash -e

#rm distribution/target/druid-0.9.0-SNAPSHOT-bin.tar.gz

mvn install -DskipTests

tar -xvf distribution/target/druid-0.9.0-SNAPSHOT-bin.tar.gz -C distribution/target/

