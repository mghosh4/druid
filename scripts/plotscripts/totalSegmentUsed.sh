#!/bin/bash -e

# Scripts needs to be run on results folder
cat historical-*-segment-used.log | awk '{print $3}' | awk '{sum+=$1} END {print sum}'