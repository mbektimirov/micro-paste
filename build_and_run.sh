#!/usr/bin/env bash
mvn package -DskipTests; java -jar micro-paste-1.0.0-fat.jar