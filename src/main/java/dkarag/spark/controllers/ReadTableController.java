package dkarag.spark.controllers;

import dkarag.spark.configs.SparkConfig;
import dkarag.spark.dtos.ReadTableOnDateRangeDTO;
import dkarag.spark.utils.TimeUtils;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.web.bind.annotation.*;
import dkarag.spark.dtos.ReadTableOnDateDTO;
import dkarag.spark.services.SparkService;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.List;


@RestController
@RequiredArgsConstructor
@RequestMapping("${server.api-prefix}/read-table")
public class ReadTableController {
    private final SparkService sparkService;
    @PostMapping(value = "on-date")
    public List<String> readTableOnDate(@RequestParam(required = false) boolean writeToParquet, @RequestBody ReadTableOnDateDTO dto) throws ParseException {
        sparkService.createSparkSession("Read table "+ dto.getTableName()+" on date: "+ dto.getDate());
        Timestamp date = TimeUtils.toTimestamp(dto.getDate());
        Dataset<Row> dataset = sparkService.sqoopTableColumnsForDate(dto.getTableName(), dto.getDateColumnName(), date, writeToParquet);
        return dataset.toJSON().collectAsList();
    }

    @PostMapping(value = "on-date-range")
    public List<String> readTableOnDateRange(@RequestParam(required = false) boolean writeToParquet, @RequestBody ReadTableOnDateRangeDTO dto) throws ParseException {
        sparkService.createSparkSession("Read table "+ dto.getTableName()+" on date range: "+ dto.getStartDate()+"-"+dto.getEndDate());
        Timestamp startDate = TimeUtils.toTimestamp(dto.getStartDate());
        Timestamp endDate = TimeUtils.toTimestamp(dto.getEndDate());
        Dataset<Row> dataset = sparkService.sqoopTableColumnsForDateRange(dto.getTableName(), dto.getDateColumnName(), startDate, endDate, writeToParquet);
        return dataset.toJSON().collectAsList();
    }


}
