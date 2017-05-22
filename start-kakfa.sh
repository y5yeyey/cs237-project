#!/bin/sh

LOCAL_KAFKA="kafka_2.11-0.10.2.0"

cd "$LOCAL_KAFKA"

bin/zookeeper-server-start.sh config/zookeeper.properties && bin/kafka-server-start.sh config/server.properties 
