package com.grecco.DeltaLake.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

public class FileGenerator {

    public static void generateSampleParquet(SparkSession sparkSession, String path) {

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("nome", DataTypes.StringType, false, Metadata.empty()),
                new StructField("idade", DataTypes.IntegerType, false, Metadata.empty())
        });

        Row row1 = RowFactory.create(1, "João", 30);
        Row row2 = RowFactory.create(2, "Maria", 25);
        Row row3 = RowFactory.create(3, "Pedro", 8);

        Dataset<Row> df = sparkSession.createDataFrame(Arrays.asList(row1, row2, row3), schema);

        df.write().mode("overwrite").parquet(path);

        System.out.println("Parquet generated at: " + path);
    }

    public static void generateDeltaTable(SparkSession sparkSession, String path) {
        // Aqui você cria o Dataset com os dados que quiser
        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("nome", DataTypes.StringType, false, Metadata.empty()),
                new StructField("idade", DataTypes.IntegerType, false, Metadata.empty())
        });

        Row row1 = RowFactory.create(1, "João", 30);
        Row row2 = RowFactory.create(2, "Maria", 25);
        Row row3 = RowFactory.create(3, "Pedro", 8);

        Dataset<Row> df = sparkSession.createDataFrame(Arrays.asList(row1, row2, row3), schema);

        // Escreve no formato Delta
        df.write()
                .mode("overwrite")
                .format("delta")
                .save(path);
    }

}
