#!/bin/bash

set -e

PG_JDBC_VERSION=42.2.8

curl -fsSL -o /tmp/postgresql.jar "https://jdbc.postgresql.org/download/postgresql-${PG_JDBC_VERSION}.jar"
sudo cp /tmp/postgresql.jar /usr/share/logstash/postgresql.jar
