#!/bin/bash
# we don't build the WAR because deploying a compressed WAR takes too much time
# better to build a standard stuff
./build.sh
rsync --del -R -Pzrlt target/dependency target/classes config/*.dev.* config/*.prd.* README.md gigasticadmin gigasticd fsqtool ceefour@luna3.bippo.co.id:gigastic/
