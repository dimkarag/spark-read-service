package dkarag.spark.controllers;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import dkarag.spark.dtos.ReadTableOnDateDTO;
import dkarag.spark.utils.SparkUtils;


@RestController
@RequiredArgsConstructor
@RequestMapping("${server.api-prefix}/read")
public class ReadTableController {

    @PostMapping(value = "on-date")
    public void readTableOnDate(@RequestBody ReadTableOnDateDTO dto){
        System.out.println("eee");
    }
}
