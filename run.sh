#!/bin/bash

echo ============= Preparation =================
hdfs dfs -mkdir -p /opt/yarn/stage
hdfs dfs -chmod -R 777 /opt/yarn
hdfs dfs -copyFromLocal ./user.profile.tags.us.txt /opt/yarn
hdfs dfs -mv /opt/yarn/user.profile.tags.us.txt /opt/yarn/src-file

echo ============ Build ====================
mvn clean package

echo ============= Run ===================
cd ./yarn-client/target
yarn jar hw2-yarn-client-1.0-SNAPSHOT.jar com.epam.training.hw2.YClient
