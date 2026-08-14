#!/bin/bash

set -e

curl -fsSL -o /tmp/postgresql.jar https://jdbc.postgresql.org/download/postgresql-42.2.8.jar
sudo cp /tmp/postgresql.jar /usr/share/logstash/postgresql.jar
