package com.grecco.DeltaLake.repository;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface DeltaLakeRepository {
    Dataset<Row> readParquet(String path);
}
