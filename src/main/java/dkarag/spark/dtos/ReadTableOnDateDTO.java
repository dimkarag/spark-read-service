package dkarag.spark.dtos;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

@Data
public class ReadTableOnDateDTO {
    private String tableName;
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
    private String date;
}
