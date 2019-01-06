#!/bin/bash

echo "start flume..."
$FLUME_HOME/bin/flume-ng agent -n a1 -c conf -f flume_kafka.template -Dflume.root.logger=TRACE,console
