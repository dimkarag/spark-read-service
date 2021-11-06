package dkarag.spark.controllers;

import dkarag.spark.services.SparkService;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;

@RestController
@RequiredArgsConstructor
@RequestMapping("${server.api-prefix}/read-parquet")
public class ReadParquetController {
    private final SparkService sparkService;

    @PostMapping
    public List<String> readParquet(@RequestParam String tableName)  {
        sparkService.createSparkSession("Read table "+ tableName +" from parquet");
        Dataset<Row> dataset = sparkService.readFromParquet(sparkService.getSparkConfig().getParquetFolderPath(), tableName);
        return dataset.toJSON().collectAsList();
    }
}
