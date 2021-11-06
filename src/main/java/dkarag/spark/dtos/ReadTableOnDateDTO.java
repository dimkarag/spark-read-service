package dkarag.spark.dtos;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class ReadTableOnDateDTO {
    private String tableName;
    private String dateColumnName;
    @JsonFormat(pattern="yyyy-MM-dd")
    private String date;
    private List<String> columnsToSelect = new ArrayList<>();

}
