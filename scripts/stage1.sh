#!/bin/bash

rm -rf output/warehouse

if [ -f "data/VED_171101_week.csv" ]; then
    echo "Dataset already Loaded.. Skipping this part"
else
    echo "Downloading Dataset from Yandex Disk..."
    bash scripts/data_collection.sh
fi

if [ -f "data/vehicles.csv" ]; then
    echo "Data already preprocessed.. Skipping this part"
else
    echo "Preprocessing Data abount Vehicles..."
    python scripts/preprocess_dataset.py
fi

echo "Building Database in PostgreSQL server..."
python scripts/build_projectdb.py

password=$(head -n 1 secrets/.psql.pass)

hdfs dfs -rm -r -f project/warehouse

echo "Importing All Tables to HDFS! (This may take some time..)"
sqoop import-all-tables --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/team15_projectdb --username team15 --password $password --compression-codec=snappy --compress --as-parquetfile --warehouse-dir=project/warehouse --m 1

echo "Copying data from HDFS"
hdfs dfs -copyToLocal project/warehouse output