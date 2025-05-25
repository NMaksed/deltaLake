package com.grecco.DeltaLake;

import org.apache.spark.sql.SparkSession;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import com.grecco.DeltaLake.utils.FileGenerator;

import java.io.File;

@SpringBootApplication
public class DeltaLakeApplication {

	public static void main(String[] args) {SpringApplication.run(DeltaLakeApplication.class, args);}

}
