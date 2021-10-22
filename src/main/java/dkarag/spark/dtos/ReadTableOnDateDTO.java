package dkarag.spark.dtos;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

@Data
public class ReadTableOnDateDTO {
    private String tableName;
    private String dateColumnName;
    @JsonFormat(pattern="yyyy-MM-dd")
    private String date;
}
