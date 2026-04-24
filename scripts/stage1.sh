#!/bin/bash

password=$(head -n 1 secrets/.psql.pass)

hdfs dfs -rm -r -f project/warehouse

sqoop import-all-tables --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/team15_projectdb --username team15 --password $password --compression-codec=snappy --compress --as-avrodatafile --warehouse-dir=project/warehouse --m 1