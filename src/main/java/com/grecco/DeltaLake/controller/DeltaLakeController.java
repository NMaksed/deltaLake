package com.grecco.DeltaLake.controller;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.grecco.DeltaLake.service.DeltaLakeService;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("api/")
public class DeltaLakeController {

    private final DeltaLakeService deltaLakeService;

    @Autowired
    public DeltaLakeController(DeltaLakeService deltaLakeService) {
        this.deltaLakeService = deltaLakeService;
    }


    @GetMapping("/parquet-data")
    public ResponseEntity<List<Map<String, Object>>> realParquetData() {
        List<Map<String, Object>> data = deltaLakeService.readParquetTable();
        return ResponseEntity.ok(data);
    }

    @GetMapping("/delta-data")
    public ResponseEntity<List<Map<String, Object>>> realDeltaData() {
        List<Map<String, Object>> data = deltaLakeService.readDeltaTable();
        return ResponseEntity.ok(data);
    }
}
