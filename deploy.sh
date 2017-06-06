#!/bin/bash -e

./scripts/deployment/stop_druid.sh -h scripts/deployment/config/getafix.conf

./scripts/deployment/start_druid.sh scripts/deployment/config/getafix.conf

ssh node-8 "screen -ls"

