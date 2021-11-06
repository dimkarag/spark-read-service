package dkarag.spark.controllers;

import dkarag.spark.dtos.ReadTableColumnsDTO;
import dkarag.spark.dtos.ReadTableColumnsWithColumnInValuesDTO;
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
    public List<String> readTableOnDate(@RequestBody ReadTableOnDateDTO dto, @RequestParam(required = false) boolean writeToParquet) throws ParseException {
        sparkService.createSparkSession("Read table "+ dto.getTableName()+" on date: "+ dto.getDate());
        Timestamp date = TimeUtils.toTimestamp(dto.getDate());
        Dataset<Row> dataset = sparkService.sqoopTableColumnsForDate(dto.getTableName(), dto.getDateColumnName(), date, writeToParquet);
        List<String> dataToJsonString = dataset.toJSON().collectAsList();
        sparkService.closeSpark();
        return dataToJsonString;
    }

    @PostMapping(value = "on-date-range")
    public List<String> readTableOnDateRange(@RequestBody ReadTableOnDateRangeDTO dto, @RequestParam(required = false) boolean writeToParquet) throws ParseException {
        sparkService.createSparkSession("Read table "+ dto.getTableName()+" on date range: "+ dto.getStartDate()+"-"+dto.getEndDate());
        Timestamp startDate = TimeUtils.toTimestamp(dto.getStartDate());
        Timestamp endDate = TimeUtils.toTimestamp(dto.getEndDate());
        Dataset<Row> dataset = sparkService.sqoopTableColumnsForDateRange(dto.getTableName(), dto.getDateColumnName(), startDate, endDate, writeToParquet);
        List<String> dataToJsonString = dataset.toJSON().collectAsList();
        sparkService.closeSpark();
        return dataToJsonString;
    }

    @PostMapping(value = "columns")
    public List<String> readTableColumns(@RequestBody ReadTableColumnsDTO dto, @RequestParam(required = false) boolean writeToParquet) throws ParseException {
        sparkService.createSparkSession("Read columns: "+ dto.getColumnNames().stream().toArray() + " from table "+ dto.getTableName());
        Dataset<Row> dataset = sparkService.sqoopSpecificColumnsOfTable(dto.getTableName(), dto.getColumnNames().toArray(new String[0]), writeToParquet);
        List<String> dataToJsonString = dataset.toJSON().collectAsList();
        sparkService.closeSpark();
        return dataToJsonString;
    }

    @PostMapping(value = "columns-with-column-value-in")
    public List<String> readTableColumnsWithColumnValueIn(@RequestBody ReadTableColumnsWithColumnInValuesDTO dto, @RequestParam(required = false) boolean writeToParquet) throws ParseException {
        sparkService.createSparkSession("Read columns: "+ dto.getColumnNames().stream().toArray() + " from table "+ dto.getTableName() +
                                                " with column "+dto.getColumnToCheckValue() + " value in " + dto.getValuesIn().toArray());
        String [] columns = null;
        if (dto.getColumnNames().size() >0) {
            columns = dto.getColumnNames().toArray(new String[0]);
        }
        Dataset<Row> dataset = sparkService.sqoopTableColumnsWithColumnInValues(dto.getTableName(), dto.getColumnToCheckValue(), dto.getValuesIn(), writeToParquet, columns);
        List<String> dataToJsonString = dataset.toJSON().collectAsList();
        sparkService.closeSpark();
        return dataToJsonString;
    }

}
