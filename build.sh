#!/usr/bin/env bash

set -e

mkdir -p target
rm -rf target/*

# Build every version of docs
current_version=snapshot

cp -R versions/versions.md versions/$current_version/docs/src/main/paradox/docs/
cd versions/$current_version && \
sbt "project docs" clean makeSite && \
cp -R docs/target/site/* ../../target


