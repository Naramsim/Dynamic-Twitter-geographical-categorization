#!/usr/bin/env bash

export SPARK_DIST_CLASSPATH=$(hadoop classpath)
export SPARK_EXECUTOR_MEMORY=2G
export SPARK_DRIVER_MEMORY=2G