#!/bin/bash -e

./scripts/deployment/stop_druid.sh -h scripts/deployment/config/getafix.conf

sleep 5

./scripts/deployment/start_druid.sh scripts/deployment/config/getafix.conf

sleep 5

ssh node-8 "screen -ls"

