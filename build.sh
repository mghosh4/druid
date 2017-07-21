#!/bin/bash -e

sudo rm -rf druid-0.9.0-SNAPSHOT

sudo mvn install -DskipTests

cp distribution/target/druid-0.9.0-SNAPSHOT-bin.tar.gz .

tar -xvf druid-0.9.0-SNAPSHOT-bin.tar.gz

