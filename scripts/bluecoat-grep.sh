#!/bin/bash
time find data/ -iname "*.log.gz" |xargs zgrep -h -F -f ~/bluecoat-domains.txt |split --verbose -d -l 1000000 - output/bluecoat/bluecoat
