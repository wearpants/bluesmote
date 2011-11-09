#!/bin/bash
time find data/ -iname "*.log.gz" |xargs zgrep -h -v OBSERVED |split --verbose -d -l 1000000 - output/blocked/blocked
