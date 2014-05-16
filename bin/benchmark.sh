#!/bin/bash

write_percent=1
read_percent=99
metric=histogram # [summary|histogram]
ops=1000000
threads=8
recordcount=1000000 # records to insert during warmup
valuesize=1024

url=tcp://192.168.0.110:6666
store=test

./bin/voldemort-performance-tool.sh --threads $threads -w $write_percent -r $read_percent  --metric-type $metric --ops-count $ops --url $url --value-size $valuesize --store-name $store --record-count $recordcount $@

