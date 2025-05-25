package com.grecco.DeltaLake.service;

import com.grecco.DeltaLake.utils.FileGenerator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.grecco.DeltaLake.repository.DeltaLakeRepository;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class DeltaLakeService {

    private DeltaLakeRepository deltaLakeRepository;
    private static final String PATH = "tmp/sample-parquet";
    private static final String DELTA_PATH = "tmp/sample-delta";
    private SparkSession spark;

    @Autowired
    public DeltaLakeService(DeltaLakeRepository deltaLakeRepository, SparkSession sparkSession) {
        this.deltaLakeRepository = deltaLakeRepository;
        this.spark = sparkSession;
    }

    public List<Map<String, Object>> readParquetTable() {
        File parquetDir = new File("tmp/sample-parquet");
        if (!parquetDir.exists()) {
            FileGenerator.generateSampleParquet(spark, PATH);
        }
        Dataset<Row> df = spark.read().parquet(PATH);
        return datasetToList(df);
    }

    public List<Map<String, Object>> readDeltaTable() {
        File parquetDir = new File("tmp/sample-delta");
        if (!parquetDir.exists()) {
            FileGenerator.generateDeltaTable(spark, DELTA_PATH);
        }
        Dataset<Row> df = spark.read().format("delta").load(DELTA_PATH);
        return datasetToList(df);
    }

    private List<Map<String, Object>> datasetToList(Dataset<Row> df) {
        String[] columns = df.columns();
        return df.toJavaRDD()
                .map(row -> {
                    Map<String, Object> map = new HashMap<>();
                    for (String col : columns) {
                        map.put(col, row.getAs(col));
                    }
                    return map;
                }).collect();
    }
}
