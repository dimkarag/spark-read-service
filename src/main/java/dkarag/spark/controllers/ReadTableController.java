package dkarag.spark.controllers;

import dkarag.spark.utils.TimeUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import dkarag.spark.dtos.ReadTableOnDateDTO;
import dkarag.spark.services.SparkService;

import java.sql.Timestamp;


@RestController
@RequiredArgsConstructor
@RequestMapping("${server.api-prefix}/read")
public class ReadTableController {
    private final SparkService sparkService;
    @PostMapping(value = "on-date")
    public void readTableOnDate(@RequestBody ReadTableOnDateDTO dto){
        sparkService.createSparkSession("Read table "+ dto.getTableName()+" on date: "+ dto.getDate());
        sparkService.sqoopTableColumnsForDate(dto.getTableName(), dto.getDateColumnName(), TimeUtils.toStartOfTheDay(Timestamp.valueOf(dto.getDate())), false);
    }
}
