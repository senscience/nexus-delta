#!/usr/bin/env bash

set -e

mkdir -p target
rm -rf target/*

# Build every version of docs
current_version=v1.11.x

for i in $(ls -d versions/*/);
do
  (
    cp -R versions/versions.md $i/docs/src/main/paradox/docs/
    cd $i && \
    sbt "project docs" clean makeSite && \
    if [ $i == versions/$current_version/ ]
    then
      cp -R docs/target/site/* ../../target
    else
      current_dir=$(basename $PWD)
      target_dir="../../target/$current_dir"
      mkdir -p $target_dir
      cp -R docs/target/site/* $target_dir
    fi
  )
done

