#!/usr/bin/env bash

ln -sf /opt/docker/plugins/disabled/project-deletion.jar /opt/docker/plugins/project-deletion.jar &&
/opt/docker/bin/delta-app -Xmx4G \
  -Dotel.service.name=nexus-delta \
  -Dotel.exporter.otlp.protocol=http/protobuf