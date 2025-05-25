package com.grecco.DeltaLake.repository;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

@Repository
public class DeltaLakeRepositoryImpl implements DeltaLakeRepository {

    private final SparkSession sparkSession;

    @Autowired
    public DeltaLakeRepositoryImpl(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    @Override
    public Dataset<Row> readParquet(String path) {
        return sparkSession.read().parquet(path);
    }
}
