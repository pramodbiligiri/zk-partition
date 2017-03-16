#!/bin/bash

export CLASSPATH=lib/jars/*:lib/bundles/*:build
bin/zkCli.sh -timeout 100000000 -server localhost:4005
